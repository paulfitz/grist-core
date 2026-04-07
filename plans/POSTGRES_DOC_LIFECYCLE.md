# How Grist Documents Load and Close

A guide for developers working on the Postgres backend. Covers both
backends side by side so you can see what changed and what stayed the
same.

## Which storage manager?

Grist has three storage managers, and the Postgres backend adds a
fourth:

| Manager | When used | What it does |
|---------|-----------|-------------|
| `HostedStorageManager` | Production (multi-user) | Syncs docs to S3/external storage, manages Redis worker assignments, handles snapshots. 895 lines. |
| `DocStorageManager` | Single-user / desktop | Simple filesystem ops. 361 lines. |
| `PgDocStorageManager` | Postgres backend (dev/test) | Schema-per-doc in Postgres. ~500 lines. Import, export, fork, replace. |
| `TrivialDocStorageManager` | Minimal stub | Throws on most operations. |

The choice happens in `FlexServer.ts:1496-1519`. Multi-user mode uses
`HostedStorageManager`. Single-user uses `DocStorageManager`. The
Postgres backend hooks into `ICreate.createLocalDocStorageManager()`
to substitute `PgDocStorageManager` when `GRIST_DOC_BACKEND=postgres`
is set.

**Important:** `PgDocStorageManager` currently replaces the LOCAL
storage manager, not the hosted one. For production Postgres
deployments, you'd need to either adapt `HostedStorageManager` to
work with Postgres or extend `PgDocStorageManager` with snapshot,
external storage, and worker-assignment support. The Postgres backend
partly simplifies this (no file copying, no S3 sync needed for the
doc data itself) but doesn't eliminate all the production concerns.

## The 30-second version

When you create or open a document, here's what happens:

```
DocManager.fetchDoc("myDoc")
  → ActiveDoc.loadDoc()
    → Is it new?
       YES → _createDocFile()     ← creates storage + metadata + Table1
       NO  → docStorage.openFile() ← opens existing storage
    → _loadOpenDoc()              ← streams metadata to Python engine
    → _initDoc()                  ← sets up ActionHistory, Sharing, ACL
    → _finishInitialization()     ← loads user tables, runs Calculate
```

The storage backend (SQLite or Postgres) only matters in the first
few steps. Once data reaches the Python engine, everything is
identical.

## Creating a new document

### Step 1: Storage creation

`_createDocFile()` at `ActiveDoc.ts:2700` does three things:

```
1. docStorage.createFile()       ← backend-specific
2. docStorage.exec(GRIST_DOC_SQL) ← creates _grist_* metadata tables
3. docStorage.run(UPDATE _grist_DocInfo SET timezone...) ← sets locale
```

**SQLite:** `createFile()` creates a `.grist` file on disk, opens it,
sets PRAGMAs (synchronous, journal_mode, trusted_schema).

**Postgres:** `createFile()` creates a schema (`doc_<name>`), creates
a `_gristsys_version` table (replaces PRAGMA user_version), then runs
`docStorageSchema.create()` to build the `_gristsys_*` system tables
(ActionHistory, Files, etc.). Uses `IF NOT EXISTS` to handle re-runs.

### Step 2: Metadata tables

`GRIST_DOC_SQL` (or `GRIST_DOC_WITH_TABLE1_SQL`) is auto-generated
SQL that creates all `_grist_*` metadata tables and seeds them with
initial data (ACL groups, DocInfo row, etc.). It's the same SQL for
both backends.

**SQLite:** Runs as-is.

**Postgres:** PgMinDB detects the `PRAGMA foreign_keys=OFF` preamble
and applies transformations:
- Quotes all `_grist*` identifiers (Postgres is case-sensitive)
- Converts `BOOLEAN → INTEGER` (metadata uses literal 0/1 in INSERTs)
- Strips `PRAGMA`, `BEGIN TRANSACTION`, `COMMIT`
- Converts `1e999 → 1e308`
- Tracks a `_gristDocSqlApplied` flag to prevent duplicate runs

The flag matters because `createFile()` and `_createDocFile()` are
separate calls. Without the flag, the SQL would run twice.

### Step 3: User table creation

If `skipInitialTable` is false (the default for normal doc creation),
`GRIST_DOC_WITH_TABLE1_SQL` also creates a `Table1` with columns
A, B, C. For the Postgres backend, this table gets native types
plus `gristAlt_*` companion columns.

## How "is it new?" works

`loadDoc()` at `ActiveDoc.ts:838` decides whether to create or open:

```typescript
const isNew = options?.forceNew ||
  await this._docManager.storageManager.prepareLocalDoc(this.docName);
```

`forceNew: true` (from `createEmptyDoc`) always creates. Otherwise
it asks the storage manager.

| Manager | How it checks |
|---------|-------------|
| `HostedStorageManager` | `_claimDocument()` — checks Redis/S3, downloads if exists. Returns `true` if nothing found anywhere. |
| `DocStorageManager` | Always returns `false` (assumes file is already there). |
| `PgDocStorageManager` | Queries `information_schema.schemata` for the schema name. Returns `true` if schema doesn't exist. |

## Opening an existing document

### Step 1: Storage open

`docStorage.openFile()` just needs to verify the storage exists and
load the schema metadata.

**SQLite:** Opens the `.grist` file, runs PRAGMAs, checks for needed
migrations, calls `_updateMetadata()`.

**Postgres:** Checks the schema exists in `information_schema`, sets
`_initialized = true`, checks schema version and runs storage
migrations if needed (same `docStorageSchema.migrations` as SQLite),
then calls `_updateMetadata()`.

`_updateMetadata()` is the same for both backends:
```sql
SELECT t."tableId", c."colId", c."type"
FROM "_grist_Tables_column" c
JOIN "_grist_Tables" t ON c."parentId" = t.id
```

This populates `_docSchema` — a map from `{tableId → {colId → gristType}}`
that PgDocStorage uses to know column types for encoding/decoding.

### Step 2: Streaming metadata to Python

`_loadOpenDoc()` at `ActiveDoc.ts:2453` does the real work:

```
1. Fetch _grist_DocInfo to check schema version
2. Run migrations if needed (schema version < SCHEMA_VERSION)
3. Start streaming metadata tables to the Python engine
4. Kick off fetching all schema tables in parallel
```

The streaming is managed by `TableMetadataLoader` — a state machine
that coordinates fetching table data from storage and pushing it to
the Python engine via `load_meta_tables` and `load_table` sandbox
calls.

The key sequence:

```
TableMetadataLoader:
  1. Fetch _grist_Tables + _grist_Tables_column
  2. Push both to Python via load_meta_tables (the "core push")
  3. Fetch remaining metadata tables (up to 3 in parallel)
  4. Push each via load_table
  5. All done → _loadOpenDoc returns
```

This is backend-agnostic. The metadata loader calls
`docStorage.fetchTable()` which returns a marshalled Buffer. For
SQLite, the native C++ `allMarshal` produces this. For Postgres,
`PgMinDB.allMarshal()` queries Postgres, pivots rows to
column-oriented format, and marshals with the JS `Marshaller`.

### Step 3: Initialization

After `_loadOpenDoc` returns, `loadDoc` continues:

```
fetchTablesAsActions()  ← wait for all metadata to finish streaming
DocData(metaTableData)  ← wrap metadata in a DocData object
_initDoc()              ← set up ActionHistory, GranularAccess, Sharing
```

`_initDoc()` at `ActiveDoc.ts:908` does:
- Creates `OnDemandActions`
- Initializes `ActionHistory` (reads branch state from
  `_gristsys_ActionHistoryBranch`)
- Sets up `GranularAccess` (ACL engine)
- Creates `Sharing` (real-time collaboration)
- Ensures at least one action in history (records initial state)

### Step 4: Finish initialization (async)

`_finishInitialization()` at `ActiveDoc.ts:3022` runs asynchronously
after `loadDoc` returns:

```
1. Wait for all metadata streaming to complete
2. Load user tables into Python engine (3 at a time)
3. Call get_table_stats (for logging)
4. Call initialize (sets up Python engine fully)
5. Run Calculate action (evaluates all formulas)
6. Set _fullyLoaded = true
```

**Important:** `loadDoc` returns BEFORE `_finishInitialization`
completes. The doc is "open" but user tables aren't loaded yet.
Any operation that needs user data must call
`waitForInitialization()` first.

## How Postgres changes the picture

### What's different

| Aspect | SQLite | Postgres |
|--------|--------|----------|
| Storage unit | `.grist` file | Postgres schema (`doc_<name>`) |
| Schema versioning | `PRAGMA user_version` | `_gristsys_version` table |
| Column types | Loose (BLOB/TEXT/etc.) | Native (boolean, date, text[], etc.) |
| Non-conforming values | Marshalled to BLOB | Stored in `gristAlt_*` companion |
| Data encoding | `_encodeValue` → marshal | `encodeNativeValue` + `encodeAltValue` |
| Data decoding | `_decodeValue` → unmarshal | `decodeNativeValue` + merge with alt |
| Connection | In-process SQLite | Dedicated pooled Postgres client |
| Transactions | SQLite serialized | Pool client with BEGIN/COMMIT |

### What's the same

Everything from `_loadOpenDoc` onward is identical. The Python engine
doesn't know which backend is in use. `TableMetadataLoader`,
`ActionHistoryImpl`, `Sharing`, `GranularAccess` — all unchanged.
They call `docStorage.fetchTable()`, `docStorage.execTransaction()`,
etc. through the `ISQLiteDB` interface that both backends implement.

### The PgMinDB translation layer

All SQL from Grist code passes through `PgMinDB` before reaching
Postgres. PgMinDB handles:

- `?` → `$1, $2, ...` (parameter style)
- `PRAGMA` → version table queries or no-ops
- `sqlite_master` → `information_schema.tables`
- `BLOB` → `BYTEA`, `DATETIME` → `TIMESTAMPTZ`
- `INTEGER PRIMARY KEY` → `GENERATED BY DEFAULT AS IDENTITY`
- `1e999` → `1e308`
- `BOOLEAN DEFAULT 0` → `BOOLEAN DEFAULT FALSE`
- `INSERT OR REPLACE` → `ON CONFLICT DO UPDATE`
- `INSERT OR IGNORE` → `ON CONFLICT DO NOTHING`
- `IFNULL` → `COALESCE`

### The dedicated client

Each PgDocStorage gets a dedicated Postgres connection from the pool.
The connection has `SET search_path TO "<schema>"` applied once on
creation, so all subsequent queries target the right schema. This
avoids re-setting search_path on every query.

Transactions use a SEPARATE pool client (acquired per transaction).
Nested transactions are detected and flattened — if you're already
in a transaction, the inner `execTransaction` just runs the callback
directly.

## Shutdown

`ActiveDoc.shutdown()` at `ActiveDoc.ts:755`:

```
1. Stop all intervals (attachment cleanup, etc.)
2. Run RemoveStaleObjects action
3. Shut down Python sandbox
4. Close docStorage
5. Remove from DocManager._activeDocs
```

**SQLite:** `docStorage.shutdown()` closes the SQLite database handle.

**Postgres:** `PgDocStorage.shutdown()` releases the dedicated pool
client back to the pool. The schema and data persist in Postgres.

## Resolved: reopen bug

The reopen bug was caused by boolean round-trip through metadata.
Metadata tables use `BOOLEAN → INTEGER` translation so `INSERT
VALUES(0)` works. On read, `_grist_Tables.onDemand` returned `0`
(integer) instead of `false` (boolean). `filterRecords({onDemand:
false})` uses lodash `isEqual(0, false)` which returns `false`, so
no tables matched and nothing was loaded into the Python engine.

**Fix**: `decodeNativeValue` for Bool columns converts integers to
booleans. See POSTGRES_TESTING_INSIGHTS.md for details.

## Resolved: fixture import

Importing `.grist` files (SQLite) into Postgres initially used
`GRIST_DOC_SQL` (latest schema) then imported old data, but
migrations conflicted with the latest schema. The fix creates tables
directly from `PRAGMA table_info` on the source SQLite file, using
correct types from `schema.ts` (metadata) or `getNativePgType` (user
tables). Storage migrations then run on `openFile()` to bring the
schema up to date. See `PgDocStorageManager.importGristFile`.

## File reference

| File | Role in lifecycle |
|------|-------------------|
| `app/server/lib/ActiveDoc.ts` | Orchestrates everything |
| `app/server/lib/TableMetadataLoader.ts` | Streams metadata to Python |
| `app/server/lib/DocStorage.ts` | SQLite storage + schema migrations |
| `app/server/lib/PgDocStorage.ts` | Postgres storage + native types |
| `app/server/lib/PgMinDB.ts` | SQL translation layer |
| `app/server/lib/PgDocStorageManager.ts` | Schema lifecycle (create/delete/rename) |
| `app/server/lib/ActionHistoryImpl.ts` | Action history (backend-agnostic) |
| `app/server/lib/MigrationUtils.ts` | Backend-agnostic migration helpers |
| `app/server/lib/initialDocSql.ts` | Auto-generated initial metadata SQL |
| `app/server/lib/ICreate.ts` | Factory that chooses backend |
