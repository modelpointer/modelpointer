use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use sqlx::{AnyPool, Row};

use modelpointer_core::model::ModelCard;
use modelpointer_core::upstream::node::{
    ApiCompatibility, ProviderType, RuntimeType, UpstreamBinding, UpstreamCredential,
    UpstreamGroup, UpstreamNode, UpstreamProfile,
};
use modelpointer_core::upstream::routing::{RoutingStrategy, RoutingStrategyConfig};

use super::DatabaseDialect;

/// Load all upstream groups from the database.
/// One `UpstreamGroup` is built per (model_id, protocol) pair.
/// Called at startup and periodically by the polling task.
pub async fn load_all_upstream_groups(
    pool: &AnyPool,
    dialect: DatabaseDialect,
) -> Result<Vec<UpstreamGroup>, String> {
    let sql = match dialect {
        DatabaseDialect::Postgres => {
            r#"
            SELECT
                mr.model_id,
                mr.strategy,
                CAST(mr.is_fallback AS INTEGER) as is_fallback,
                mr.weight,
                mr.upstream_model_name,
                pn.id as provider_node_id,
                pn.base_url,
                pn.api_compatibility,
                p.api_key,
                p.name as provider_name,
                p.provider_type,
                m.key_rpm,
                m.key_tpm,
                m.model_rpm,
                m.model_tpm
            FROM model_routes mr
            JOIN provider_nodes pn ON pn.id = mr.provider_node_id
            JOIN providers p ON p.id = pn.provider_id
            JOIN models m ON m.id = mr.model_id
            WHERE m.status != 'disabled'
            ORDER BY mr.model_id, pn.api_compatibility, mr.is_fallback, mr.weight DESC
        "#
        }
        _ => {
            r#"
            SELECT
                mr.model_id,
                mr.strategy,
                mr.is_fallback,
                mr.weight,
                mr.upstream_model_name,
                pn.id as provider_node_id,
                pn.base_url,
                pn.api_compatibility,
                p.api_key,
                p.name as provider_name,
                p.provider_type,
                m.key_rpm,
                m.key_tpm,
                m.model_rpm,
                m.model_tpm
            FROM model_routes mr
            JOIN provider_nodes pn ON pn.id = mr.provider_node_id
            JOIN providers p ON p.id = pn.provider_id
            JOIN models m ON m.id = mr.model_id
            WHERE m.status != 'disabled'
            ORDER BY mr.model_id, pn.api_compatibility, mr.is_fallback, mr.weight DESC
        "#
        }
    };

    let rows = sqlx::query(sql)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("DB error loading model routes: {e}"))?;

    if rows.is_empty() {
        return Ok(vec![]);
    }

    let alias_rows = sqlx::query("SELECT alias, model_id FROM model_aliases")
        .fetch_all(pool)
        .await
        .map_err(|e| format!("DB error loading model aliases: {e}"))?;

    let mut aliases_map: HashMap<String, Vec<String>> = HashMap::new();
    for row in alias_rows {
        let model_id: String = row.get("model_id");
        let alias: String = row.get("alias");
        aliases_map.entry(model_id).or_default().push(alias);
    }

    // Load per-protocol primary-tier capacity from model_protocol_capacity.
    // Keyed by (model_id, protocol) -> (capacity_rpm, capacity_tpm).
    let capacity_rows = sqlx::query(
        "SELECT model_id, protocol, capacity_rpm, capacity_tpm FROM model_protocol_capacity",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| format!("DB error loading model_protocol_capacity: {e}"))?;

    let mut capacity_map: HashMap<(String, String), (Option<u32>, Option<u32>)> = HashMap::new();
    for row in capacity_rows {
        let model_id: String = row.get("model_id");
        let protocol: String = row.get("protocol");
        let cap_rpm: Option<u32> = row
            .try_get::<i64, _>("capacity_rpm")
            .ok()
            .map(|v| u32::try_from(v).unwrap_or(u32::MAX));
        let cap_tpm: Option<u32> = row
            .try_get::<i64, _>("capacity_tpm")
            .ok()
            .map(|v| u32::try_from(v).unwrap_or(u32::MAX));
        capacity_map.insert((model_id, protocol), (cap_rpm, cap_tpm));
    }

    struct RouteRow {
        strategy: String,
        is_fallback: bool,
        weight: i64,
        provider_node_id: String,
        base_url: String,
        api_compatibility: String,
        api_key: Option<String>,
        provider_name: String,
        provider_type: Option<String>,
        upstream_model_name: Option<String>,
        key_rpm: Option<i64>,
        key_tpm: Option<i64>,
        model_rpm: Option<i64>,
        model_tpm: Option<i64>,
    }

    // Group rows by (model_id, api_compatibility) — one group per protocol.
    let mut model_proto_rows: HashMap<(String, String), Vec<RouteRow>> = HashMap::new();
    for row in rows {
        let model_id: String = row.get("model_id");
        let api_compatibility: String = row.get("api_compatibility");
        let is_fallback: i64 = row.try_get::<i64, _>("is_fallback").unwrap_or(0);
        model_proto_rows
            .entry((model_id, api_compatibility.clone()))
            .or_default()
            .push(RouteRow {
                strategy: row.get::<String, _>("strategy"),
                is_fallback: is_fallback != 0,
                weight: row.get::<i64, _>("weight"),
                provider_node_id: row.get::<String, _>("provider_node_id"),
                base_url: row.get::<String, _>("base_url"),
                api_compatibility,
                api_key: row.try_get::<String, _>("api_key").ok(),
                provider_name: row.get::<String, _>("provider_name"),
                provider_type: row.try_get::<String, _>("provider_type").ok(),
                upstream_model_name: row.try_get::<String, _>("upstream_model_name").ok(),
                key_rpm: row.try_get::<i64, _>("key_rpm").ok(),
                key_tpm: row.try_get::<i64, _>("key_tpm").ok(),
                model_rpm: row.try_get::<i64, _>("model_rpm").ok(),
                model_tpm: row.try_get::<i64, _>("model_tpm").ok(),
            });
    }

    let mut groups = Vec::new();
    for ((model_id, protocol), rows) in model_proto_rows {
        let strategy_str = rows.first().map(|r| r.strategy.as_str()).unwrap_or("swrr");
        let routing_strategy = match strategy_str {
            "weighted_hash" => RoutingStrategy::WeightedHash,
            _ => RoutingStrategy::Swrr,
        };

        // Rate limit fields are model-level; read from first row.
        let key_rpm = rows
            .first()
            .and_then(|r| r.key_rpm)
            .map(|v| u32::try_from(v).unwrap_or(u32::MAX));
        let key_tpm = rows
            .first()
            .and_then(|r| r.key_tpm)
            .map(|v| u32::try_from(v).unwrap_or(u32::MAX));
        let model_rpm = rows
            .first()
            .and_then(|r| r.model_rpm)
            .map(|v| u32::try_from(v).unwrap_or(u32::MAX));
        let model_tpm = rows
            .first()
            .and_then(|r| r.model_tpm)
            .map(|v| u32::try_from(v).unwrap_or(u32::MAX));

        // Per-protocol primary-tier capacity.
        let (primary_capacity_rpm, primary_capacity_tpm) = capacity_map
            .get(&(model_id.clone(), protocol.clone()))
            .copied()
            .unwrap_or((None, None));

        // Aliases are shared across all protocols for a model; clone for each group.
        let aliases = aliases_map.get(&model_id).cloned().unwrap_or_default();
        let model_card = ModelCard::new(model_id.clone()).with_aliases(aliases);

        let bindings: Result<Vec<UpstreamBinding>, String> = rows
            .into_iter()
            .map(|r| {
                let api_compat: ApiCompatibility = r
                    .api_compatibility
                    .parse()
                    .unwrap_or(ApiCompatibility::OpenAi);

                let node = UpstreamNode {
                    profile: UpstreamProfile {
                        base_url: r.base_url,
                        provider_node_id: r.provider_node_id,
                        api_compatibility: api_compat,
                        runtime_type: RuntimeType::External,
                        credential: Arc::new(UpstreamCredential {
                            name: r.provider_name,
                            api_key: r.api_key,
                            provider_type: r
                                .provider_type
                                .as_deref()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(ProviderType::Unknown),
                        }),
                        upstream_model_name: r.upstream_model_name,
                    },
                    healthy: Arc::new(AtomicBool::new(true)),
                };

                let weight = (r.weight.clamp(1, 255)) as u8;
                let strategy_config = match routing_strategy {
                    RoutingStrategy::WeightedHash => RoutingStrategyConfig::WeightedHash { weight },
                    RoutingStrategy::Swrr => RoutingStrategyConfig::Swrr { weight },
                };
                let priority = if r.is_fallback { 1 } else { 0 };

                UpstreamBinding::new(node, true, strategy_config, priority)
            })
            .collect();

        let bindings = bindings?;

        match UpstreamGroup::new(model_card, routing_strategy, bindings) {
            Ok(group) => groups.push(
                group
                    .with_rate_limits(key_rpm, key_tpm, model_rpm, model_tpm)
                    .with_primary_capacity(primary_capacity_rpm, primary_capacity_tpm),
            ),
            Err(e) => tracing::warn!(
                "Skipping model '{}' protocol '{}': {}",
                model_id,
                protocol,
                e
            ),
        }
    }

    Ok(groups)
}
