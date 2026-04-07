# Testing Insights: Postgres Backend

What we learned by running existing Grist test suites against the
Postgres backend. Each section is a class of problem that surfaced,
why it matters, and how it was resolved.

## Two storage formats, not one

Grist has two consumers of stored data: the Python engine (via
`load_table`) and the Node read path (via `fetchQuery` /
`decodeMarshalledData`). They expect different formats.

**Python** expects the SQLite storage format: primitive values as-is,
complex values (lists, dicts, errors) as marshalled binary blobs.
A Grist list `['L', 3, 5, 6]` is stored as a binary blob in SQLite.
Python unmarshals the blob and gets the list directly.

**Node** expects Grist application format: lists as `['L', 3, 5, 6]`
JS arrays, errors as `['E', 'TypeError']` arrays, etc.

With native Postgres types, these diverge. An `integer[]` column
stores `{3, 5, 6}` in Postgres. Node reads it as `[3, 5, 6]` and
adds the `'L'` tag. But Python receives raw `[3, 5, 6]` from the
marshal buffer and doesn't add the tag — it expects the blob to
already contain it.

**Solution**: `fetchTable` (the Python path) uses
`_mergeAltColumnsInBuffer` which keeps bytea alt values as raw
Buffers and re-marshals native arrays with the `'L'` tag as blobs.
`decodeMarshalledData` (the Node path) uses `decodeNativeValue`
which converts to Grist application format. Two different decode
paths for two different consumers.

## The alt column must use marshal, not JSON

The first implementation used `jsonb` for alt companion columns.
This seemed natural — jsonb is queryable, flexible, and
human-readable in psql.

Tests revealed that `JSON.stringify` loses information:
- `Infinity` → `"null"` (the string, not JS null)
- `NaN` → `"null"`
- `-0` → `"0"`
- Nested Grist types like `['L', ['O', {A: 1}]]` lose structure

Switching to `bytea` with Grist's marshal format preserves
everything. The alt column is opaque to external tools, but that's
acceptable — it's the escape hatch for non-conforming values that
external tools can't use anyway.

## Postgres is strict about types in unexpected places

SQLite's loose typing means you can put anything anywhere and it
works. Postgres surfaces type mismatches that SQLite silently
accepts:

- **CTE type inference**: `VALUES(?)` in a recursive CTE —
  Postgres can't infer the type. Fix: `VALUES(CAST(? AS integer))`.
- **`IS NOT` operator**: SQLite's `IS NOT $N` → Postgres needs
  `IS DISTINCT FROM`. But `IS DISTINCT FROM $N` also fails if the
  parameter is untyped. Fix: `IS DISTINCT FROM CAST($N AS integer)`.
- **`IN ()`**: SQLite accepts empty `IN ()` (always false). Postgres
  rejects it. Fix: early return for empty arrays.
- **Aggregate + ORDER BY**: `SELECT count(*) ... ORDER BY col` —
  SQLite ignores the ORDER BY in aggregate queries. Postgres
  requires the column in GROUP BY. Fix: skip ORDER BY for aggregate
  selections.
- **Column defaults and ALTER TYPE**: Changing a boolean column to
  integer fails if the boolean default (`FALSE`) can't auto-cast.
  Fix: DROP DEFAULT, ALTER TYPE, SET new DEFAULT.

## Boolean round-trips through metadata are fragile

Metadata tables use `BOOLEAN → INTEGER` translation (so `INSERT
VALUES(0)` works). But when reading metadata back, the integer `0`
must become `false` (boolean) for `filterRecords({ onDemand: false })`
to match. lodash `isEqual(0, false)` returns `false`.

This caused the reopen bug: after shutdown and restart,
`_grist_Tables.onDemand` was `0` instead of `false`, so
`filterRecords` returned no tables, and no user tables were loaded
into the Python engine.

**Fix**: `decodeNativeValue` for Bool converts integers to booleans.
**Lesson**: any place that stores booleans as integers needs explicit
conversion on read.

## Name uniqueness can't use the filesystem

`DocManager._createNewDoc` uses `createExclusive(path)` — a
filesystem operation — to claim a unique doc name. For Postgres,
`getPath()` returned a schema name, and `createExclusive` created
a real file in the working directory. This caused:

1. Hundreds of dummy files (`doc_calculate_attribution`,
   `doc_calculate_attribution_2`, etc.) accumulating
2. Name mismatches: `createDoc("foo")` → `foo-7` (file exists),
   then `loadDoc("foo")` opens stale schema `doc_foo`

**Fix**: `IDocStorageManager.docExists?` optional interface method.
`PgDocStorageManager` returns `false` (any name is available).
`PgDocStorage.createFile` drops stale schemas before creating.

## On-demand tables need alt columns in partial fetches

The `AlternateActions` system (for on-demand tables) generates undo
data by fetching specific columns: `fetchActionData(table, rowIds,
['Birth_Date', 'lname'])`. This partial fetch didn't include
`gristAlt_Birth_Date`, so undo values for non-conforming cells
were captured as NULL instead of the actual alt values.

Similarly, `expandQuery` for on-demand tables builds explicit
`SELECT` lists that don't include alt columns. Both paths needed
alt companions injected.

## Date and DateTime need lossless round-trip checks

Postgres `date` has day precision. A Grist Date value like `678`
(epoch seconds, not midnight) becomes `1970-01-01` in Postgres
and reads back as `0`. The value is lost.

Postgres `timestamptz` has millisecond precision. A value like
`5e-324` (Number.MIN_VALUE) becomes epoch 0 after
`new Date(5e-324 * 1000)`. The value is lost.

**Fix**: Only store in native columns if the conversion is
lossless (`val % 86400 === 0` for Date, `d.getTime()/1000 === val`
for DateTime). Non-lossless values go to alt.

## Infinity works natively in Postgres numeric

Postgres `numeric` supports `'Infinity'` and `'-Infinity'` as
values. This means `ManualSortPos` and `PositionNumber` columns
(which default to Infinity) can use the native column directly —
no need for alt. The column DEFAULT can be `'Infinity'` and the
Python engine reads it correctly.

NaN and `-0` still can't be stored natively (NaN isn't supported
by numeric, -0 is indistinguishable from 0). These go to alt.

## GRIST_DOC_SQL creates user tables too

The auto-generated `GRIST_DOC_WITH_TABLE1_SQL` creates `Table1`
with SQLite-style column types. PgMinDB translates the types
(BLOB → BYTEA, etc.) but didn't add alt companion columns. This
meant `Table1` created via the initial SQL had no `gristAlt_A`
column, and subsequent writes to non-conforming values failed.

**Fix**: During GRIST_DOC_SQL processing, detect `CREATE TABLE`
for non-metadata tables and inject alt companion columns.

## Type translation must respect quoted identifiers

`translateSql` replaced `BLOB` → `BYTEA` and `DATETIME` →
`TIMESTAMPTZ` globally. But a user column named `"Blob"` or
`"DateTime"` would get its NAME replaced too, producing columns
like `"BYTEA"` instead of `"Blob"`.

**Fix**: Only apply type translations inside DDL statements
(CREATE/ALTER), and use lookbehind/lookahead to skip replacements
inside double quotes.

## Schema lifecycle matters for test repeatability

Tests must be repeatable. With SQLite, each test run uses a fresh
temp directory. With Postgres, schemas persist across runs. Every
`CREATE TABLE IF NOT EXISTS`, `INSERT`, and `CREATE INDEX` must be
idempotent (using `IF NOT EXISTS`, `OR IGNORE`, etc.).

But idempotency isn't enough — stale data from previous runs
causes incorrect test results. `PgDocStorage.createFile` now
drops the schema before recreating, ensuring a clean slate.

## Transaction abort on duplicate key

Postgres aborts the entire transaction when any statement fails.
SQLite just returns an error and continues. This matters for
attachment inserts: `INSERT INTO _gristsys_FileInfo` might fail
on a duplicate key (same file uploaded twice). In SQLite, the
`INSERT OR IGNORE` silently continues. In Postgres, the whole
transaction is aborted.

**Fix**: Use `SAVEPOINT` around attachment inserts. If the INSERT
fails, `ROLLBACK TO SAVEPOINT` preserves the transaction.

## Stack overflow with large parameter sets

SQLite handles `...params` spread for thousands of parameters.
Postgres `all(sql, ...params)` with 200k+ filter values overflows
the call stack because JavaScript can't spread that many arguments.

**Fix**: Added `allMarshalArray(sql, params[])` which accepts
params as an array directly. Also added `ANY($N::type[])` syntax
for large filter sets (passes the entire array as one parameter).

## Fixture import must not assume latest schema

The initial import approach ran `GRIST_DOC_SQL` (latest metadata
schema) then copied data from the old `.grist` file. This breaks
when importing older files — the latest schema has columns the
old data doesn't have, and migrations assume they're upgrading
from an older version, not the latest.

**Fix**: Create tables from `PRAGMA table_info` on the source
SQLite file, using types from `schema.ts` for metadata tables
and `getNativePgType` for user tables. Then run storage
migrations via `openFile()` to bring the schema current. This
mirrors how SQLite handles old files: open as-is, then migrate.

## The `pg` library processes all statements

Unlike node-sqlite3 (which only executes the first statement in
a string), the `pg` library's `client.query(sql, [])` uses the
simple query protocol and executes ALL semicolon-separated
statements. This means the SQL endpoint's `select * from (...)`
wrapping alone doesn't prevent multi-statement injection — a user
can close the paren, add a semicolon, and append arbitrary SQL.

Named prepared statements force the extended query protocol,
which rejects multiple statements. This needs to be applied to
the SQL endpoint path specifically (not all queries, as it has
overhead from prepared statement caching).

## Postgres→SQLite export for downloads and forks

Several features (download as .grist, fork with filtering, document
replace) need a SQLite file even when the backend is Postgres.
`PgDocStorageManager.exportToGristFile` creates a temporary `.grist`
file by:
1. Opening a fresh SQLite file with `docStorageSchema.create()`
2. Querying each Postgres table's columns via `information_schema`
3. Creating matching SQLite tables (reversing the type mapping)
4. Copying data row by row, encoding native values back to SQLite format
5. Returning the temp file path for the caller to stream/process

Fork filtering uses `filterDocumentInPlaceWithDB` which operates
on the exported SQLite file to apply access control rules before
sending to the client.

## `isMetadataTable` must match both prefixes

Grist has two internal table prefixes: `_grist_*` (metadata like
`_grist_Tables`) and `_gristsys_*` (system like `_gristsys_Files`).
The `isMetadataTable` check initially used `startsWith('_grist_')`
which missed `_gristsys_*` tables. BulkAddRecord for system tables
went through the user-table code path (which adds alt columns),
causing double-import failures.

**Fix**: `startsWith('_grist')` matches both prefixes.
