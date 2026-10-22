use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};
use url::{ParseError, Url};

/// A parsed URL whose formatting never exposes credentials in its authority.
/// Serialization preserves credentials and must not be used for logging.
#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct RedactedUrl(Url);

impl<'de> Deserialize<'de> for RedactedUrl {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let input = String::deserialize(deserializer)?;
        Self::parse(&input).map_err(serde::de::Error::custom)
    }
}

impl RedactedUrl {
    pub fn parse(input: &str) -> Result<Self, ParseError> {
        let url = Url::parse(input)?;
        // Opaque URLs can hide credentials in their path rather than userinfo.
        if url.cannot_be_a_base() {
            return Err(ParseError::RelativeUrlWithoutBase);
        }
        Ok(Self(url))
    }

    /// Accept the scheme-less addresses supported by NATS and Redis Sentinel.
    pub fn with_default_scheme(input: &str, scheme: &str) -> Result<Self, ParseError> {
        if input.contains("://") {
            Self::parse(input)
        } else {
            Self::parse(&format!("{scheme}://{input}"))
        }
    }

    pub fn comma_separated_with_default_scheme(
        input: &str,
        scheme: &str,
    ) -> Result<Vec<Self>, ParseError> {
        input
            .split(',')
            .map(|url| Self::with_default_scheme(url, scheme))
            .collect()
    }

    /// Expose the unredacted URL only at serialization or connection boundaries.
    pub fn expose_url(&self) -> &Url {
        &self.0
    }
}

pub fn join_comma_separated_urls(urls: &[RedactedUrl]) -> String {
    urls.iter()
        .map(|url| url.expose_url().as_str())
        .collect::<Vec<_>>()
        .join(",")
}

impl FromStr for RedactedUrl {
    type Err = ParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::parse(input)
    }
}

impl fmt::Debug for RedactedUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut url = self.0.clone();
        if !url.username().is_empty() || url.password().is_some() {
            if url.set_password(None).is_err() || url.set_username("REDACTED").is_err() {
                return f.write_str("<redacted url>");
            }
        }
        f.write_str(url.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formatting_redacts_userinfo_without_changing_connection_url() {
        for input in [
            "nats://user:secret@host:4222",
            "nats://secret@host:4222",
            "redis://user:sec%40ret@[::1]:6379/0",
            "redis://user:sec@ret@host:6379/0",
            "https://user:secret@example.com/token",
        ] {
            let url = RedactedUrl::parse(input).unwrap();
            for formatted in [format!("{url:?}"), format!("{url:#?}")] {
                assert!(!formatted.contains("secret"));
                assert!(!formatted.contains("sec%40ret"));
                assert!(!formatted.contains("sec%40ret%40"));
                assert!(formatted.contains("REDACTED"));
            }
            assert_eq!(url.expose_url(), &Url::parse(input).unwrap());
        }
    }

    #[test]
    fn preserves_noncredential_components() {
        let url = RedactedUrl::parse("redis://host:6379/path@name?key=value#part").unwrap();
        assert_eq!(format!("{url:?}"), url.expose_url().as_str());
        let url = RedactedUrl::with_default_scheme("localhost:4222", "nats").unwrap();
        assert_eq!(url.expose_url().as_str(), "nats://localhost:4222");
        assert!(RedactedUrl::parse("nats://user:secret@[invalid").is_err());
        assert!(RedactedUrl::parse("opaque:secret").is_err());
        assert!(serde_json::from_str::<RedactedUrl>("\"opaque:secret\"").is_err());
    }

    #[test]
    fn serde_preserves_credentials() {
        let url = RedactedUrl::parse("redis://user:secret@host:6379/0").unwrap();
        let serialized = serde_json::to_string(&url).unwrap();
        assert!(serialized.contains("secret"));
        assert_eq!(
            serde_json::from_str::<RedactedUrl>(&serialized).unwrap(),
            url
        );
    }

    #[test]
    fn comma_separated_urls_with_scheme_split_correctly() {
        let test = "nats://a:4222,nats://b:4222";

        let expected = vec![
            RedactedUrl::parse("nats://a:4222").unwrap(),
            RedactedUrl::parse("nats://b:4222").unwrap(),
        ];
        let actual = RedactedUrl::comma_separated_with_default_scheme(test, "nats").unwrap();
        assert_eq!(actual, expected);
    }
    #[test]
    fn comma_separated_urls_without_uniform_scheme_split_correctly() {
        let test = "nats://a:4222,b:4222";
        let expected = vec![
            RedactedUrl::parse("nats://a:4222").unwrap(),
            RedactedUrl::with_default_scheme("b:4222", "nats").unwrap(),
        ];
        let actual = RedactedUrl::comma_separated_with_default_scheme(test, "nats").unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn comma_separated_urls_with_user_and_pass_split_correctly() {
        let test = "u:p@a,nats://u:p@b:4222";
        let expected = vec![
            RedactedUrl::with_default_scheme("u:p@a", "nats").unwrap(),
            RedactedUrl::with_default_scheme("u:p@b:4222", "nats").unwrap(),
        ];
        let actual = RedactedUrl::comma_separated_with_default_scheme(test, "nats").unwrap();
        assert_eq!(actual, expected);
    }
}
