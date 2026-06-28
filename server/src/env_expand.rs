/// Expand `${VAR_NAME}` placeholders in `s` using environment variables.
/// Returns an error if a referenced variable is not set or the syntax is invalid.
pub fn expand_env(s: &str) -> Result<String, String> {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let var: String = chars.by_ref().take_while(|&c| c != '}').collect();
            if var.is_empty() {
                return Err("Empty variable reference ${}".to_string());
            }
            let val = std::env::var(&var)
                .map_err(|_| format!("Environment variable '{}' is not set", var))?;
            result.push_str(&val);
        } else {
            result.push(c);
        }
    }
    Ok(result)
}
