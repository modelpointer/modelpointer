use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use sqlx::{AnyPool, Row};

use crate::model::ModelCard;
use crate::upstream::node::{
    ApiCompatibility, UpstreamBinding, UpstreamGroup, UpstreamNode, UpstreamProfile,
    UpstreamCredential, ProviderType, RuntimeType,
};
use crate::upstream::routing::{RoutingStrategy, RoutingStrategyConfig};
use super::db::DatabaseDialect;

#[derive(Debug, Clone)]
pub struct ModelRouteRecord {
    pub id: String,
    pub model_id: String,
    pub provider_node_id: String,
    pub protocol: String,
    pub is_fallback: bool,
    pub strategy: String,
    pub weight: i64,
}

pub struct NewModelRoute {
    pub provider_node_id: String,
    pub protocol: String,
    pub is_fallback: bool,
    pub strategy: String,
    pub weight: i64,
}

pub struct ModelRouteStore {
    pool: AnyPool,
    dialect: DatabaseDialect,
}

impl ModelRouteStore {
    pub fn new(pool: AnyPool, dialect: DatabaseDialect) -> Self {
        Self { pool, dialect }
    }

    pub async fn list_by_model(&self, model_id: &str) -> Result<Vec<ModelRouteRecord>, sqlx::Error> {
        let sql = match self.dialect {
            DatabaseDialect::Postgres => "SELECT id, model_id, provider_node_id, protocol, CAST(is_fallback AS INTEGER) as is_fallback, strategy, weight FROM model_routes WHERE model_id = $1 ORDER BY is_fallback, weight DESC",
            _ => "SELECT id, model_id, provider_node_id, protocol, is_fallback, strategy, weight FROM model_routes WHERE model_id = ? ORDER BY is_fallback, weight DESC",
        };
        let rows = sqlx::query(sql).bind(model_id).fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(map_route_row).collect())
    }

    pub async fn save_routes(&self, model_id: &str, routes: &[NewModelRoute]) -> Result<(), sqlx::Error> {
        let del_sql = match self.dialect {
            DatabaseDialect::Postgres => "DELETE FROM model_routes WHERE model_id = $1",
            _ => "DELETE FROM model_routes WHERE model_id = ?",
        };
        sqlx::query(del_sql).bind(model_id).execute(&self.pool).await?;

        for route in routes {
            let id = uuid::Uuid::new_v4().to_string();
            let sql = match self.dialect {
                DatabaseDialect::Postgres => "INSERT INTO model_routes (id, model_id, provider_node_id, protocol, is_fallback, strategy, weight) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                _ => "INSERT INTO model_routes (id, model_id, provider_node_id, protocol, is_fallback, strategy, weight) VALUES (?, ?, ?, ?, ?, ?, ?)",
            };
            sqlx::query(sql)
                .bind(&id)
                .bind(model_id)
                .bind(&route.provider_node_id)
                .bind(&route.protocol)
                .bind(route.is_fallback as i32)
                .bind(&route.strategy)
                .bind(route.weight)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }
}

fn map_route_row(row: sqlx::any::AnyRow) -> ModelRouteRecord {
    ModelRouteRecord {
        id: row.get::<String, _>("id"),
        model_id: row.get::<String, _>("model_id"),
        provider_node_id: row.get::<String, _>("provider_node_id"),
        protocol: row.get::<String, _>("protocol"),
        is_fallback: row.get::<i32, _>("is_fallback") != 0,
        strategy: row.get::<String, _>("strategy"),
        weight: row.get::<i64, _>("weight"),
    }
}

/// Load all model worker groups from the database.
/// Called at startup and periodically by the polling task.
pub async fn load_all_upstream_groups(
    pool: &AnyPool,
    dialect: DatabaseDialect,
) -> Result<Vec<UpstreamGroup>, String> {
    let sql = match dialect {
        DatabaseDialect::Postgres => r#"
            SELECT
                mr.model_id,
                mr.strategy,
                CAST(mr.is_fallback AS INTEGER) as is_fallback,
                mr.weight,
                pn.base_url,
                pn.api_compatibility,
                p.api_key,
                p.provider_type
            FROM model_routes mr
            JOIN provider_nodes pn ON pn.id = mr.provider_node_id
            JOIN providers p ON p.id = pn.provider_id
            JOIN models m ON m.id = mr.model_id
            WHERE m.status != 'disabled'
            ORDER BY mr.model_id, mr.is_fallback, mr.weight DESC
        "#,
        _ => r#"
            SELECT
                mr.model_id,
                mr.strategy,
                mr.is_fallback,
                mr.weight,
                pn.base_url,
                pn.api_compatibility,
                p.api_key,
                p.provider_type
            FROM model_routes mr
            JOIN provider_nodes pn ON pn.id = mr.provider_node_id
            JOIN providers p ON p.id = pn.provider_id
            JOIN models m ON m.id = mr.model_id
            WHERE m.status != 'disabled'
            ORDER BY mr.model_id, mr.is_fallback, mr.weight DESC
        "#,
    };

    let rows = sqlx::query(sql)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("DB error loading model routes: {}", e))?;

    if rows.is_empty() {
        return Ok(vec![]);
    }

    // Load all aliases
    let alias_rows = sqlx::query("SELECT alias, model_id FROM model_aliases")
        .fetch_all(pool)
        .await
        .map_err(|e| format!("DB error loading model aliases: {}", e))?;

    let mut aliases_map: HashMap<String, Vec<String>> = HashMap::new();
    for row in alias_rows {
        let model_id: String = row.get("model_id");
        let alias: String = row.get("alias");
        aliases_map.entry(model_id).or_default().push(alias);
    }

    // Group route rows by model_id
    struct RouteRow {
        strategy: String,
        weight: i64,
        base_url: String,
        api_compatibility: String,
        api_key: Option<String>,
        provider_type: Option<String>,
    }

    let mut model_rows: HashMap<String, Vec<RouteRow>> = HashMap::new();
    for row in rows {
        let model_id: String = row.get("model_id");
        model_rows.entry(model_id).or_default().push(RouteRow {
            strategy: row.get::<String, _>("strategy"),
            weight: row.get::<i64, _>("weight"),
            base_url: row.get::<String, _>("base_url"),
            api_compatibility: row.get::<String, _>("api_compatibility"),
            api_key: row.try_get::<String, _>("api_key").ok(),
            provider_type: row.try_get::<String, _>("provider_type").ok(),
        });
    }

    let mut groups = Vec::new();
    for (model_id, rows) in model_rows {
        // Use strategy from the first row (all rows in the same group share strategy)
        let strategy_str = rows.first().map(|r| r.strategy.as_str()).unwrap_or("swrr");
        let routing_strategy = match strategy_str {
            "weighted_hash" => RoutingStrategy::WeightedHash,
            _ => RoutingStrategy::Swrr,
        };

        let aliases = aliases_map.remove(&model_id).unwrap_or_default();
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
                        api_compatibility: api_compat,
                        runtime_type: RuntimeType::External,
                        credential: Arc::new(UpstreamCredential {
                            name: model_id.clone(),
                            api_key: r.api_key,
                            provider_type: r.provider_type
                                .as_deref()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(ProviderType::Unknown),
                        }),
                    },
                    healthy: Arc::new(AtomicBool::new(true)),
                };

                let weight = (r.weight.clamp(1, 255)) as u8;
                let strategy_config = match routing_strategy {
                    RoutingStrategy::WeightedHash => RoutingStrategyConfig::WeightedHash { weight },
                    RoutingStrategy::Swrr => RoutingStrategyConfig::Swrr { weight },
                };

                UpstreamBinding::new(node, true, strategy_config)
            })
            .collect();

        let bindings = bindings?;

        match UpstreamGroup::new(model_card, routing_strategy, bindings) {
            Ok(group) => groups.push(group),
            Err(e) => tracing::warn!("Skipping model '{}': {}", model_id, e),
        }
    }

    Ok(groups)
}
