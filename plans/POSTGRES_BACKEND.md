# Postgres Backend for Grist Documents

## The idea

Replace SQLite as the storage engine for Grist documents with
PostgreSQL, so that each document's tables live as regular Postgres
tables in a shared database. The home database already supports
Postgres; this is about the *document data* path.

## Design decisions

### 1. Postgres as a queryable database, not just a storage swap

The Postgres tables must be directly queryable by external tools —
BI dashboards, Python notebooks, other applications. This means:

- Table and column names match what users see in the Grist UI
- Column types are native Postgres types where possible
- The schema is effectively a public interface
- `_grist_*` metadata tables are present but clearly prefixed so
  external users can ignore them

### 2. Strict and non-strict columns

Grist currently allows any value in any cell — put "banana" in a
numeric column and it's stored as-is (shown in red, but preserved).
SQLite's loose typing makes this free. Postgres's strict typing does
not.

**Strict columns** (new concept) enforce their type at the Grist
engine level. Non-conforming values are rejected on input. In
Postgres, a strict column is a single native-typed column:

```sql
"Salary" numeric NOT NULL DEFAULT 0
```

**Non-strict columns** (current Grist behavior) preserve any value.
In Postgres, this requires a column pair:

```sql
"Salary" numeric,       -- native-typed, NULL when non-conforming
"Salary_" jsonb,        -- raw Grist value (source of truth)
```

External tools query the native-typed column. Grist reads/writes the
raw companion for non-strict columns. When the value conforms to the
type, both columns agree. When it doesn't, the native column is NULL.

Strict mode needs: a per-column UI toggle, engine-level enforcement,
and a data-cleaning workflow for switching a column to strict. It's a
Grist concept that applies to both backends, but is especially
valuable for Postgres-backed documents.

#### Type mapping

| Grist Type     | Strict Postgres     | Non-strict pair             |
|---------------|--------------------|-----------------------------|
| Numeric       | `numeric`          | `numeric` + `jsonb`         |
| Integer       | `integer`          | `integer` + `jsonb`         |
| Text          | `text`             | `text` + `jsonb`            |
| Bool          | `boolean`          | `boolean` + `jsonb`         |
| Date          | `date`             | `date` + `jsonb`            |
| DateTime      | `timestamptz`      | `timestamptz` + `jsonb`     |
| Reference     | `integer` (FK)     | `integer` + `jsonb`         |
| ReferenceList | `integer[]`        | `integer[]` + `jsonb`       |
| Choice        | `text`             | `text` + `jsonb`            |
| ChoiceList    | `text[]`           | `text[]` + `jsonb`          |
| Attachments   | `jsonb`            | `jsonb` (no pair needed)    |
| Any           | `jsonb`            | `jsonb` (no pair needed)    |

#### Open questions on column mapping

- Naming for the raw companion: `_` suffix is terse but could collide
  with user column names. `_raw`? `__grist_raw`?
- Should raw companions live in a separate schema?
- Should the native-typed column be generated/computed from the raw
  column, or separately maintained?

### 3. Single ActiveDoc per document (unchanged)

Keep the existing model where each open document has one ActiveDoc
instance on one worker. Postgres improves HA — failover means a new
worker opens a Postgres connection instead of copying a SQLite file
or cleaning stale Redis locks (#1639). Multiple readers bypassing
ActiveDoc for read-only queries is a natural follow-on but not in
initial scope.

### 4. .grist files become an export format

Postgres is the live store. "Download as .grist" exports to SQLite.
The .grist format remains the portable interchange format for
emailing documents and importing into SQLite-backed instances.
No dual-write.

### 5. Per-deployment configuration

A Grist instance is configured to use Postgres for document storage
(or not). All documents use the configured backend. SQLite remains
the default — self-hosters who don't need external SQL access or
scaling change nothing. Postgres is opt-in.

### 6. Schema-per-document (default)

Each document gets a Postgres schema: `doc_xxx."Employees"`. Provides
namespace isolation, `DROP SCHEMA CASCADE` for deletion, and clean
table names when external tools set `search_path`. Alternatively,
prefixed tables in one schema (`doc_xxx__Employees`) for simpler
deployments. Configurable, default to schema-per-document.

### 7. Metadata and attachments mirror existing structure

`_grist_*` metadata tables keep their existing names and live in the
document's schema. `_gristsys_Files` stores attachments in `bytea`
for internal storage, with the same S3/MinIO external storage
integration as today.

### 8. Infrastructure

- Shared `node-postgres` connection pool per worker (not per
  document). PgBouncer optional for larger deployments.
- Home database and document data can share a Postgres instance
  (different databases or schemas).
- Per-document CLI migration tool (.grist to Postgres and back).
  Admins script bulk migration if needed.

### 9. Performance (deferred)

- `allMarshal()` equivalent: start with standard `node-postgres` →
  JS objects → marshal. Measure, optimize later (COPY BINARY, custom
  type parsers).
- Small-document latency: accepted tradeoff. Measure once the path
  exists.

## Why people want this

From GitHub issues and community discussion:

- **External tool access** (#667): Users want to point BI tools,
  Python notebooks, PyGWalker, etc. directly at their data.
- **Horizontal scaling / HA** (#434, #1114, #1639): Documents are
  pinned to one worker because only one process can safely write a
  SQLite file. Stale Redis assignments cause downtime on pod restart.
- **Large datasets** (#43, #898, #901): SQLite documents hit
  practical limits around 100K-200K rows.
- **Operational overhead** (#1163, #1122): Action history bloats
  SQLite files (500KB of data → 900MB with history).

## Current architecture

```
Python Data Engine (in-memory, no direct SQLite access)
    ↕  marshalled data via _pyCall
ActiveDoc (Node.js, one per open document)
    ↕  ISQLiteDB interface
DocStorage (document-specific CRUD, action application)
    ↕  MinDB interface
SQLiteDB → @gristlabs/sqlite3 → SQLite C library
```

Key files:
- `app/server/lib/DocStorage.ts` — 85KB, the heart of document
  persistence. Action application, table CRUD, attachment storage,
  schema migrations (9 versions), marshalling.
- `app/server/lib/SQLiteDB.ts` — Promise wrapper, transaction
  nesting, `allMarshal()`.
- `app/server/lib/SqliteCommon.ts` — `MinDB` and `SqliteVariant`
  interfaces.
- `app/server/lib/ActionHistoryImpl.ts` — action log stored in
  `_gristsys_ActionHistory` tables inside the SQLite file.
- `app/server/lib/ActiveDoc.ts` — orchestrates Python engine,
  storage, and real-time updates.

The Python engine never touches SQLite directly — it works with
in-memory data and emits DocActions that ActiveDoc applies to
storage. The sandbox doesn't need to change.

## What makes this hard

1. **DocStorage is deeply SQLite-specific.** PRAGMA user_version for
   schema versioning, `allMarshal()` for binary serialization, native
   backup API, type affinity assumptions, SQL variable limit chunking.
   This is 85KB of code that can't be trivially adapted.

2. **Action history is co-located with data.** It lives in
   `_gristsys_ActionHistory` in the same SQLite file. The existing
   code assumes data and history share a transaction boundary. With
   Postgres, separating history (as proposed in #1122) would reduce
   scope but requires rethinking the transactional guarantees.

3. **Snapshots.** Today a snapshot is a copy of the SQLite file via
   the native backup API. With Postgres, snapshots become `pg_dump`
   per document (or a logical mechanism). The `DocSnapshots` system
   that uploads to S3 would need to handle dumps instead of files.

4. **Formulas constrain concurrency.** The Python engine recalculates
   on every change and holds all data in memory. All writes must
   funnel through one engine instance. Postgres doesn't change this
   bottleneck. (This is why we keep single-ActiveDoc for now.)

## Implementation status

### Done

- **PgMinDB** (`app/server/lib/PgMinDB.ts`): Postgres adapter for
  MinDB interface. SQL translation (parameter binding, PRAGMA
  interception, type mapping: BLOB→BYTEA, BOOLEAN→BOOLEAN,
  DATETIME→TIMESTAMPTZ, INTEGER PRIMARY KEY→GENERATED BY DEFAULT
  AS IDENTITY, 1e999→1e308). Implements `allMarshal()` in JS using
  the Marshaller class. Converts Postgres-native types on read
  (Date→epoch seconds). Dedicated connection per document with
  connection-pool-based transactions.

- **PgDocStorage** (`app/server/lib/PgDocStorage.ts`): Standalone
  class implementing DocStorage's public interface for Postgres.
  Schema-per-document. All DocAction processors (AddTable,
  BulkAddRecord, UpdateRecord, RemoveRecord, AddColumn,
  RenameColumn, ModifyColumn, RemoveColumn, RenameTable,
  RemoveTable). Nested transaction support. Uses
  `docStorageSchema.create()` + `GRIST_DOC_SQL` (with identifier
  quoting) for schema creation — single source of truth shared
  with SQLite.

- **MigrationUtils** (`app/server/lib/MigrationUtils.ts`):
  Backend-agnostic helpers for storage migrations: `listTables`,
  `listColumns`, `rebuildTable`, `tableExists`. SQLite path uses
  original DocStorage table-rebuild code. Postgres path uses
  ALTER COLUMN. Existing DocStorage migrations rewritten to use
  these helpers — all 6 SQLite migration tests pass.

- **ActionHistoryImpl**: All SQL identifiers quoted with
  `quoteIdent()` for Postgres case-sensitivity.

- **ActiveDoc**: Conditional PgDocStorage creation via
  `GRIST_DOC_BACKEND=postgres` env var.

- **Native Postgres types** for metadata tables: `BOOLEAN`,
  `TIMESTAMPTZ`, `BYTEA` (translated from SQLite's BOOLEAN,
  DATETIME, BLOB by PgMinDB).

- **Tests**: 5 passing tests covering basic CRUD, bulk operations,
  all column types (Text, Int, Numeric, Bool, Date, DateTime),
  schema changes (AddColumn, RenameColumn, RemoveColumn), and
  formula evaluation. Plus all 6 existing SQLite migration tests
  pass.

### Remaining work

- **Table1 creation**: `GRIST_DOC_WITH_TABLE1_SQL` is skipped by
  PgMinDB's PRAGMA guard, so new documents don't get the default
  Table1. Needs the same quoting treatment as GRIST_DOC_SQL.

- **Full server (HTTP API) test**: The `createDocTools` tests pass
  but the HTTP API path (TestServer + axios) hasn't been verified
  yet.

- **Column-pair model for user data**: Native Postgres types for
  user data columns (the strict/non-strict column concept from the
  design decisions section). Currently user data columns use the
  same types as SQLite.

- **Snapshot and backup support**: Replace SQLite backup API with
  `pg_dump` per document schema. Integrate with DocSnapshots/S3.

- **Migration tooling**: CLI tool to convert .grist files to/from
  Postgres. PoC converters exist in `plans/grist2pg.ts` and
  `plans/pg2grist.ts`.

- **Attachment support**: Currently stubbed out in PgDocStorage.

- **IDocStorageManager**: Postgres-aware storage manager for
  document lifecycle (list, delete, rename, backup).
