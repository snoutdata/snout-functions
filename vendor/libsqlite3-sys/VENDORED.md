# libsqlite3-sys 0.38.2, with SQLite 3.54.1

The crates.io release 0.38.2 (MIT, see LICENSE), unchanged except:

- `sqlite3/sqlite3.c`, `sqlite3/sqlite3.h` and `sqlite3/sqlite3ext.h` are SQLite 3.54.1, from
  https://sqlite.org/2026/sqlite-amalgamation-3540100.zip (SHA3-256
  `92281c86e2957f9b9305890f9ac04204caa7b677cc868dc326d942689df28e30`), for RUSTSEC-2026-0459.
- `sqlcipher/` is left out: no crate here turns on `bundled-sqlcipher`.

The pregenerated bindings are still 3.53.2's, a subset of 3.54.1's API. Remove this directory and
the `[patch.crates-io]` entry in `../../Cargo.toml` when the deno crates move to rusqlite 0.41.
