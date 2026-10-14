//! `verify_jwt`: a function that requires a key is run only for a request whose `Authorization`
//! is a token signed with the project's own secret and not expired.
//!
//! The front door checks the `apikey` header against the key hashes it holds (hashes only, so
//! it cannot check a signature) and passes `Authorization` through. Until 0.2.2 nothing checked it,
//! so a function marked "key required" ran for any caller holding the public anon key, whatever
//! token it sent: a forged, unsigned `service_role` token reached the function, and a function
//! written for the upstream gateway that reads the user from the token's claims was impersonable. The
//! host already holds each project's secret for Realtime, so the manifest carries it here and the
//! runtime checks it before a function's process is even started.
//!
//! HS256 only, which is what every project's keys and its auth server's tokens are signed with. The
//! secret never reaches a project's own process (router.rs strips it).

use base64::Engine;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Base64url with or without padding: encoders differ, and the signature covers the text as sent.
const URL: GeneralPurpose = GeneralPurpose::new(&base64::alphabet::URL_SAFE, GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent));

#[derive(Debug, PartialEq, Eq)]
pub enum Refused {
	/// No `Authorization: Bearer` at all.
	Missing,
	/// One that is not a token, not HS256, not signed with this secret, or expired.
	Invalid,
}

impl Refused {
	pub fn message(&self) -> &'static str {
		match self {
			Refused::Missing => "Missing authorization header",
			Refused::Invalid => "Invalid JWT",
		}
	}
}

/// Whether `authorization` (the header's value) carries a token this secret signed that has not
/// expired by `now` (seconds since the epoch).
pub fn check(authorization: Option<&[u8]>, secret: &str, now: u64) -> Result<(), Refused> {
	let value = authorization.and_then(|v| std::str::from_utf8(v).ok()).map(str::trim).ok_or(Refused::Missing)?;
	let (scheme, token) = value.split_once(' ').ok_or(Refused::Missing)?;
	if !scheme.eq_ignore_ascii_case("bearer") {
		return Err(Refused::Missing);
	}
	let token = token.trim();
	let mut parts = token.split('.');
	let (Some(header), Some(payload), Some(signature), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
		return Err(Refused::Invalid);
	};
	let header: serde_json::Value = URL.decode(header).ok().and_then(|bytes| serde_json::from_slice(&bytes).ok()).ok_or(Refused::Invalid)?;
	if header.get("alg").and_then(|v| v.as_str()) != Some("HS256") {
		return Err(Refused::Invalid);
	}
	let signature = URL.decode(signature).map_err(|_| Refused::Invalid)?;
	let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|_| Refused::Invalid)?;
	mac.update(&token.as_bytes()[..header_and_payload_len(token)]);
	mac.verify_slice(&signature).map_err(|_| Refused::Invalid)?;
	let claims: serde_json::Value = URL.decode(payload).ok().and_then(|bytes| serde_json::from_slice(&bytes).ok()).ok_or(Refused::Invalid)?;
	if let Some(exp) = claims.get("exp") {
		let exp = exp.as_f64().ok_or(Refused::Invalid)?;
		if exp <= now as f64 {
			return Err(Refused::Invalid);
		}
	}
	Ok(())
}

/// The signed part of a token: everything before its last dot.
fn header_and_payload_len(token: &str) -> usize {
	token.rfind('.').unwrap_or(0)
}

pub fn now() -> u64 {
	std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn sign(header: &str, claims: &str, secret: &str) -> String {
		let signed = format!("{}.{}", URL.encode(header), URL.encode(claims));
		let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("a key");
		mac.update(signed.as_bytes());
		let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
		format!("{signed}.{signature}")
	}

	const HS256: &str = r#"{"alg":"HS256","typ":"JWT"}"#;

	fn bearer(token: &str) -> Vec<u8> {
		format!("Bearer {token}").into_bytes()
	}

	#[test]
	fn a_token_this_secret_signed_passes_until_it_expires() {
		let token = sign(HS256, r#"{"role":"anon","exp":2000}"#, "s3cret");
		assert_eq!(check(Some(&bearer(&token)), "s3cret", 1999), Ok(()));
		assert_eq!(check(Some(&bearer(&token)), "s3cret", 2000), Err(Refused::Invalid));
		// The scheme is case-insensitive, as HTTP's is.
		assert_eq!(check(Some(format!("bearer {token}").as_bytes()), "s3cret", 0), Ok(()));
		// No `exp` is no expiry, as PostgREST reads it.
		let forever = sign(HS256, r#"{"role":"anon"}"#, "s3cret");
		assert_eq!(check(Some(&bearer(&forever)), "s3cret", u64::MAX / 2), Ok(()));
	}

	#[test]
	fn a_forged_or_foreign_token_is_refused() {
		// The live case: claims of service_role with no real signature.
		let forged = format!("{}.{}.AAAA", URL.encode(HS256), URL.encode(r#"{"role":"service_role","exp":9999999999}"#));
		assert_eq!(check(Some(&bearer(&forged)), "s3cret", 0), Err(Refused::Invalid));
		// Another project's secret.
		let other = sign(HS256, r#"{"role":"service_role"}"#, "other");
		assert_eq!(check(Some(&bearer(&other)), "s3cret", 0), Err(Refused::Invalid));
		// A changed payload under the original signature.
		let real = sign(HS256, r#"{"role":"anon"}"#, "s3cret");
		let mut parts: Vec<&str> = real.split('.').collect();
		let swapped = URL.encode(r#"{"role":"service_role"}"#);
		parts[1] = &swapped;
		assert_eq!(check(Some(&bearer(&parts.join("."))), "s3cret", 0), Err(Refused::Invalid));
		// alg none, and another algorithm.
		let none = format!("{}.{}.", URL.encode(r#"{"alg":"none"}"#), URL.encode(r#"{"role":"service_role"}"#));
		assert_eq!(check(Some(&bearer(&none)), "s3cret", 0), Err(Refused::Invalid));
		let rs = sign(r#"{"alg":"RS256"}"#, r#"{"role":"anon"}"#, "s3cret");
		assert_eq!(check(Some(&bearer(&rs)), "s3cret", 0), Err(Refused::Invalid));
		// A string expiry is not a number we can compare.
		let odd = sign(HS256, r#"{"exp":"never"}"#, "s3cret");
		assert_eq!(check(Some(&bearer(&odd)), "s3cret", 0), Err(Refused::Invalid));
	}

	#[test]
	fn something_that_is_not_a_bearer_token() {
		assert_eq!(check(None, "s3cret", 0), Err(Refused::Missing));
		assert_eq!(check(Some(b"Basic abc"), "s3cret", 0), Err(Refused::Missing));
		assert_eq!(check(Some(b"Bearer"), "s3cret", 0), Err(Refused::Missing));
		assert_eq!(check(Some(b"Bearer nope"), "s3cret", 0), Err(Refused::Invalid));
		assert_eq!(check(Some(b"Bearer a.b.c.d"), "s3cret", 0), Err(Refused::Invalid));
	}
}
