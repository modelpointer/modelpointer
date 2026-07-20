use axum::http::HeaderValue;

use crate::upstream::ApiCompatibility;

/// Apply provider-specific headers to request
pub fn apply_provider_headers(
    mut req: reqwest::RequestBuilder,
    api_compatibility: &ApiCompatibility,
    auth_header: Option<&HeaderValue>,
) -> reqwest::RequestBuilder {
    match api_compatibility {
        ApiCompatibility::Anthropic => {
            // Anthropic requires x-api-key instead of Authorization
            // Extract Bearer token and use as x-api-key
            if let Some(auth) = auth_header {
                if let Ok(auth_str) = auth.to_str() {
                    let api_key = auth_str.strip_prefix("Bearer ").unwrap_or(auth_str);
                    req = req
                        .header("x-api-key", api_key)
                        .header("anthropic-version", "2023-06-01");
                }
            }
        }
        ApiCompatibility::OpenAi => {
            // Standard OpenAI-compatible: use Authorization header as-is
            if let Some(auth) = auth_header {
                req = req.header("Authorization", auth);
            }
        }
        ApiCompatibility::Native => {
            // Provider-specific: pass the Authorization header; the caller is
            // responsible for any additional provider-specific headers.
            if let Some(auth) = auth_header {
                req = req.header("Authorization", auth);
            }
        }
    }

    req
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_anthropic_headers_uses_x_api_key() {
        let client = reqwest::Client::new();
        let auth = HeaderValue::from_static("Bearer test-key");

        let request = apply_provider_headers(
            client.post("https://example.com/messages"),
            &ApiCompatibility::Anthropic,
            Some(&auth),
        )
        .build()
        .unwrap();

        assert_eq!(request.headers().get("x-api-key").unwrap(), "test-key");
        assert_eq!(
            request.headers().get("anthropic-version").unwrap(),
            "2023-06-01"
        );
        assert!(request.headers().get("authorization").is_none());
    }

    #[test]
    fn apply_openai_headers_uses_authorization() {
        let client = reqwest::Client::new();
        let auth = HeaderValue::from_static("Bearer test-key");

        let request = apply_provider_headers(
            client.post("https://example.com/chat/completions"),
            &ApiCompatibility::OpenAi,
            Some(&auth),
        )
        .build()
        .unwrap();

        assert_eq!(
            request.headers().get("authorization").unwrap(),
            "Bearer test-key"
        );
        assert!(request.headers().get("x-api-key").is_none());
    }
}
