# hickory-proto 0.25.2, with four upstream fixes

The crates.io release 0.25.2 (MIT or Apache-2.0, see LICENSE-MIT and LICENSE-APACHE; crate
checksum `f8a6fe56c0038198998a6f217ca4e7ef3a5e51f46163bd6dd60b5c71ca6c6502`), unchanged except
for these fixes from 0.26.2, carried by hand where 0.25.2's code differs:

- RUSTSEC-2026-0487 (hickory-dns 80eaf9e7): `src/serialize/txt/rdata_parsers/svcb.rs`,
  `parse_alpn` returns a malformed list as an error instead of panicking.
- RUSTSEC-2026-0489 (c663b60a): the same file, a parameter value that is a single quote is an
  error instead of a panic on slicing.
- RUSTSEC-2026-0490 (0494e15c): `src/serialize/txt/zone_lex.rs`, the lexer has no 4,096-character
  token limit that asserted.
- RUSTSEC-2026-0491 (280c0e5c): `src/op/message.rs`, the question and record vectors start at
  `min(count, 8)` instead of the header's counts (three places in 0.25.2, two upstream).

Each fix has a test next to it (`invalid_alpn_token`, `single_quote_value`, `long_token`,
`counts_beyond_the_message_are_an_error`); the crate's own tests need hickory-dns's unpublished
`test_support`, so the four were also run from an outside crate against this copy (all pass) and
against the release (the three text-parsing ones panic). `Cargo.toml` gains an empty `[workspace]`
so the crate is not taken for a member of `../../Cargo.toml`. The release's `tests/`, `benches/` and
`Cargo.toml.orig` are left out (and their `[[test]]`/`[[bench]]` targets with them): only the
crate's own tests used them, and its DNSSEC test keys may not go into a public repository. Remove this directory and its `[patch.crates-io]` entry
in `../../Cargo.toml` when the deno crates move to hickory-proto 0.26.2 or later.
