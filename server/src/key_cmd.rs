//! CLI key management commands (`modelpointer key ...`).

use rand::Rng;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::auth_config::{AuthConfigSource, RawApiKey, RawAuthConfig};

const KEY_PREFIX: &str = "sk-";
const KEY_RANDOM_LEN: usize = 48;
const KEY_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

fn generate_api_key() -> String {
    let mut rng = rand::rng();
    let random_part: String = (0..KEY_RANDOM_LEN)
        .map(|_| KEY_CHARS[rng.random_range(0..KEY_CHARS.len())] as char)
        .collect();
    format!("{KEY_PREFIX}{random_part}")
}

fn sha256_hex(input: &str) -> String {
    format!("{:x}", Sha256::digest(input.as_bytes()))
}

// ── generate ──────────────────────────────────────────────────────────────────

pub fn cmd_generate(name: String, append: Option<String>) -> Result<(), String> {
    let id = Uuid::new_v4().to_string();
    let key = generate_api_key();
    let hash = sha256_hex(&key);

    println!();
    println!("Generated API Key");
    println!("{}", "─".repeat(64));
    println!("Key:  {key}");
    println!("ID:   {id}");
    println!("Name: {name}");
    println!("Hash: {hash}");

    if let Some(file_path) = append {
        append_to_file(&file_path, &id, &name, &hash)?;
        println!();
        println!("Appended to: {file_path}");
    } else {
        println!();
        println!("Add this entry to your auth.yaml:");
        println!();
        println!("  - id: \"{id}\"");
        println!("    name: \"{name}\"");
        println!("    hash: \"{hash}\"");
    }

    println!();
    println!("WARNING: Store the key securely — it cannot be recovered after this point.");

    Ok(())
}

fn append_to_file(file_path: &str, id: &str, name: &str, hash: &str) -> Result<(), String> {
    let path = std::path::Path::new(file_path);

    let mut config = if path.exists() {
        AuthConfigSource::new(path).load_raw()?
    } else {
        RawAuthConfig {
            mode: "api_key".to_string(),
            keys: vec![],
        }
    };

    if config.mode == "none" {
        return Err(format!(
            "auth.mode is 'none' in '{file_path}'. Change mode to 'api_key' before adding keys."
        ));
    }

    config.keys.push(RawApiKey {
        id: id.to_string(),
        name: Some(name.to_string()),
        hash: Some(hash.to_string()),
        plain: None,
        disabled: false,
    });

    AuthConfigSource::new(path).save_raw(&config)
}

// ── disable / enable ──────────────────────────────────────────────────────────

pub fn cmd_disable(id: String, file: String) -> Result<(), String> {
    set_disabled(&id, &file, true)
}

pub fn cmd_enable(id: String, file: String) -> Result<(), String> {
    set_disabled(&id, &file, false)
}

fn set_disabled(id: &str, file_path: &str, disabled: bool) -> Result<(), String> {
    let source = AuthConfigSource::new(file_path);
    let mut config = source.load_raw()?;

    let entry = config
        .keys
        .iter_mut()
        .find(|k| k.id == id)
        .ok_or_else(|| format!("Key '{id}' not found in '{file_path}'"))?;

    if entry.disabled == disabled {
        let state = if disabled {
            "already disabled"
        } else {
            "already active"
        };
        println!("Key '{id}' is {state}.");
        return Ok(());
    }

    let name = entry.name.clone().unwrap_or_else(|| id.to_string());
    entry.disabled = disabled;
    source.save_raw(&config)?;

    let action = if disabled { "disabled" } else { "enabled" };
    println!("Key '{name}' ({id}) has been {action}.");
    println!("File updated: {file_path}");
    println!("Changes take effect within the next reload interval.");

    Ok(())
}

// ── list ──────────────────────────────────────────────────────────────────────

pub fn cmd_list(file: String) -> Result<(), String> {
    let config = AuthConfigSource::new(&file).load_raw()?;

    println!("File: {file}");
    println!("Mode: {}", config.mode);

    if config.keys.is_empty() {
        println!("No keys defined.");
        return Ok(());
    }

    println!();
    println!("{:<38}  {:<24}  STATUS", "ID", "NAME");
    println!("{}", "─".repeat(72));

    for key in &config.keys {
        let name = key.name.as_deref().unwrap_or("-");
        let status = if key.disabled { "disabled" } else { "active" };
        println!("{:<38}  {:<24}  {}", key.id, name, status);
    }

    Ok(())
}
