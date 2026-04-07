/**
 * PgDocStorageManager — IDocStorageManager for Postgres-backed documents.
 *
 * Each document is a Postgres schema (doc_<docname>). Document lifecycle
 * operations (list, delete, rename) translate to schema operations.
 */

// @ts-ignore
import {Pool} from 'pg';

import {DocEntry} from 'app/common/DocListAPI';
import {DocSnapshots} from 'app/common/DocSnapshot';
import {DocumentUsage} from 'app/common/DocUsage';
import {
  EmptySnapshotProgress,
  IDocStorageManager,
  SnapshotProgress,
} from 'app/server/lib/IDocStorageManager';
import log from 'app/server/lib/log';

function docNameToSchema(docName: string): string {
  return 'doc_' + docName.replace(/[^a-zA-Z0-9_]/g, '_').slice(0, 60).toLowerCase();
}

export class PgDocStorageManager implements IDocStorageManager {
  private _pool: Pool;

  constructor(pgPool: Pool) {
    this._pool = pgPool;
  }

  public getPath(docName: string): string {
    return docNameToSchema(docName);
  }

  public async docExists(docName: string): Promise<boolean> {
    // Called by DocManager._createNewDoc during name selection for new docs.
    // Any pre-existing schema is stale (from a prior run or abandoned doc).
    // Drop it so the name can be reused cleanly.
    const schema = docNameToSchema(docName);
    const result = await this._pool.query(
      `SELECT 1 FROM information_schema.schemata WHERE schema_name = $1`, [schema]
    );
    if (result.rows.length > 0) {
      await this._pool.query(`DROP SCHEMA "${schema}" CASCADE`);
    }
    return false;
  }

  public getSQLiteDB(_docName: string) {
    return undefined;  // No SQLite for Postgres-backed docs
  }

  public getSampleDocPath(_sampleDocName: string): string | null {
    return null;  // Samples not supported for Postgres backend yet
  }

  public async getCanonicalDocName(altDocName: string): Promise<string> {
    return altDocName;
  }

  public async prepareLocalDoc(docName: string): Promise<boolean> {
    // Check if the schema exists. Return true if it's a new document.
    const schema = docNameToSchema(docName);
    const result = await this._pool.query(
      `SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1`,
      [schema]
    );
    return result.rows.length === 0;  // true = new document
  }

  public async prepareToCreateDoc(docName: string): Promise<void> {
    // Drop any stale schema so the subsequent createFile starts fresh.
    const schema = docNameToSchema(docName);
    await this._pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  }

  /**
   * Import a .grist (SQLite) file into a Postgres schema.
   *
   * The approach: replicate the SQLite structure exactly using the same code
   * paths that normal doc creation uses. For each table, read PRAGMA table_info
   * from SQLite, then create the Postgres table using the correct type mapping:
   * - System tables (_gristsys_*): created via docStorageSchema.create()
   * - Metadata tables (_grist_*): created with SQLite-compatible types via columnDef()
   * - User tables: created via _process_AddTable (native types + alt columns)
   *
   * The schema version is set to the fixture's actual version so migrations
   * run correctly against the imported data.
   */
  public async importGristFile(docName: string, gristPath: string): Promise<void> {
    const {PgDocStorage, columnDef: pgColumnDef} = require('app/server/lib/PgDocStorage');
    const marshal = require('app/common/marshal');
    // @ts-ignore
    const sqlite3 = require('@gristlabs/sqlite3');

    const db = await new Promise<any>((resolve, reject) => {
      const d = new sqlite3.Database(gristPath, sqlite3.OPEN_READONLY, (err: any) =>
        err ? reject(err) : resolve(d));
    });
    const allSql = (sql: string): Promise<any[]> => new Promise((resolve, reject) =>
      db.all(sql, (err: any, rows: any[]) => err ? reject(err) : resolve(rows)));

    // 1. Create bare schema + _gristsys_version with the fixture's version
    await this.prepareToCreateDoc(docName);
    const schema = docNameToSchema(docName);
    const storage = new PgDocStorage(this, docName, this._pool);
    await this._pool.query(`CREATE SCHEMA "${schema}"`);

    const userVersion = await allSql('PRAGMA user_version');
    const version = userVersion[0]?.user_version || 0;
    await storage.exec(`CREATE TABLE IF NOT EXISTS "_gristsys_version" (
      id INTEGER PRIMARY KEY CHECK (id = 0), version INTEGER DEFAULT 0)`);
    await storage.exec(`INSERT INTO "_gristsys_version" VALUES(0, ${version}) ON CONFLICT DO NOTHING`);

    // 2. Read column type metadata from the SQLite file
    const gristTables = await allSql('SELECT id, "tableId" FROM "_grist_Tables"');
    const gristCols = await allSql(
      'SELECT "parentId", "colId", "type" FROM "_grist_Tables_column" ORDER BY "parentPos"'
    );
    const tableIdMap = new Map<number, string>();
    for (const t of gristTables) { tableIdMap.set(t.id, t.tableId); }
    const colTypeMap = new Map<string, Map<string, string>>();
    for (const c of gristCols) {
      const tableId = tableIdMap.get(c.parentId);
      if (!tableId) { continue; }
      if (!colTypeMap.has(tableId)) { colTypeMap.set(tableId, new Map()); }
      colTypeMap.get(tableId)!.set(c.colId, c.type);
    }

    // 4. Create tables from SQLite structure
    const sqliteTables = await allSql(
      "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    );
    const gristSchema = require('app/common/schema').schema;
    for (const {name: tableId} of sqliteTables) {
      const pragmaCols = await allSql(`PRAGMA table_info("${tableId}")`);
      const cols = pragmaCols.filter((c: any) => c.name && c.name !== 'id');

      if (tableId.startsWith('_grist')) {
        // Metadata or system table: use Grist types from schema.ts when available.
        // For unknown columns (not in schema.ts), quote the name and use the
        // SQLite declared type which PgMinDB will translate.
        const metaColTypes: Record<string, string> = gristSchema[tableId] || {};
        const colSpecSql = cols.length > 0
          ? ', ' + cols.map((c: any) => {
              // First check schema.ts (for metadata table columns), then _grist_Tables_column
              const gristType = metaColTypes[c.name] || colTypeMap.get(tableId)?.get(c.name) || null;
              if (gristType) {
                return pgColumnDef(c.name, gristType);
              }
              // Unknown column — use raw SQLite type, let PgMinDB translate
              const dflt = c.dflt_value ?? 'NULL';
              return `"${c.name}" ${c.type || 'BLOB'} DEFAULT ${dflt}`;
            }).join(', ')
          : '';
        // Use the same id column pattern as the original table.
        // Most have INTEGER PRIMARY KEY (identity); some have CHECK constraints.
        const idCol = pragmaCols.find((c: any) => c.name === 'id');
        const idDef = (idCol && idCol.pk) ? 'id INTEGER PRIMARY KEY' : 'id INTEGER';
        await storage.exec(
          `CREATE TABLE "${tableId}" (${idDef}${colSpecSql})`
        );
      } else {
        // User table: native Postgres types + alt companions via _process_AddTable
        const colTypes = colTypeMap.get(tableId) || new Map<string, string>();
        const colSpecs = cols.map((c: any) => ({
          id: c.name,
          type: colTypes.get(c.name) || 'Any',
        }));
        await storage.applyStoredAction(['AddTable', tableId, colSpecs] as any);
      }
    }

    // 4. Create indexes from SQLite (with identifier quoting)
    const sqliteIndexes = await allSql(
      "SELECT sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"
    );
    for (const {sql: ddl} of sqliteIndexes) {
      // Quote _grist* table names and camelCase column names in index definitions.
      // For index column lists like (pluginId, key), quote each identifier individually.
      const translated = ddl
        .replace(/(?<!")(_grist\w+)(?!")/g, (_m: string, name: string) => `"${name}"`)
        .replace(/\(([^)]+)\)\s*$/, (_m: string, cols: string) => {
          const quoted = cols.split(',').map(c => {
            const t = c.trim();
            return /[A-Z]/.test(t) && !t.startsWith('"') ? `"${t}"` : t;
          }).join(', ');
          return `(${quoted})`;
        });
      try {
        await storage.exec(translated);
      } catch { /* index may not apply to Postgres schema */ }
    }

    // 5. Copy all data — metadata tables first so _docSchema is populated
    const sysTableNames = sqliteTables
      .filter((t: any) => t.name.startsWith('_gristsys'))
      .map((t: any) => t.name);
    const metaTableNames = sqliteTables
      .filter((t: any) => t.name.startsWith('_grist') && !t.name.startsWith('_gristsys'))
      .map((t: any) => t.name);
    const userTableNames = sqliteTables
      .filter((t: any) => !t.name.startsWith('_grist'))
      .map((t: any) => t.name);

    for (const tableId of [...sysTableNames, ...metaTableNames, ...userTableNames]) {
      const rows = await allSql(`SELECT * FROM "${tableId}"`);
      if (rows.length === 0) { continue; }

      // Decode marshalled BLOB values from SQLite.
      // For _gristsys_* tables, keep raw Buffers (BYTEA columns store binary data as-is).
      // For _grist_* and user tables, decode marshalled values to JS objects.
      const shouldDecode = !tableId.startsWith('_gristsys');
      for (const row of rows) {
        for (const key of Object.keys(row)) {
          if (Buffer.isBuffer(row[key]) && shouldDecode) {
            try { row[key] = marshal.loads(row[key]); } catch { /* keep as-is */ }
          }
        }
      }

      const colIds = Object.keys(rows[0]).filter(c => c !== 'id');
      const rowIds = rows.map((r: any) => r.id);
      const colValues: any = {};
      for (const c of colIds) {
        colValues[c] = rows.map((r: any) => r[c]);
      }
      await storage.applyStoredAction(
        ['BulkAddRecord', tableId, rowIds, colValues] as any
      );
    }

    await this._resetIdentitySequences(schema);

    await new Promise<void>((resolve, reject) =>
      db.close((err: any) => err ? reject(err) : resolve()));
    await storage.shutdown();

    log.info('PgDocStorageManager: imported %s into schema %s', gristPath, schema);
  }

  public async prepareFork(srcDocName: string, destDocName: string): Promise<string> {
    // Copy schema by creating a new schema and copying all tables
    const srcSchema = docNameToSchema(srcDocName);
    const destSchema = docNameToSchema(destDocName);

    // Create the destination schema
    await this._pool.query(`CREATE SCHEMA IF NOT EXISTS "${destSchema}"`);

    // Get all tables in the source schema
    const tables = await this._pool.query(
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = $1 AND table_type = 'BASE TABLE'`,
      [srcSchema]
    );

    for (const {table_name} of tables.rows) {
      await this._pool.query(
        `CREATE TABLE "${destSchema}"."${table_name}" (LIKE "${srcSchema}"."${table_name}" INCLUDING ALL)`
      );
      await this._pool.query(
        `INSERT INTO "${destSchema}"."${table_name}" SELECT * FROM "${srcSchema}"."${table_name}"`
      );
    }
    await this._resetIdentitySequences(destSchema);

    return destDocName;
  }

  /**
   * Reset identity sequences on all tables in a schema to max(id)+1.
   * Prevents duplicate key errors when new rows are inserted after copying data.
   */
  private async _resetIdentitySequences(schema: string): Promise<void> {
    const tables = await this._pool.query(
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = $1 AND table_type = 'BASE TABLE'`, [schema]
    );
    for (const {table_name} of tables.rows) {
      const identityCheck = await this._pool.query(
        `SELECT 1 FROM information_schema.columns
         WHERE table_schema = $1 AND table_name = $2 AND column_name = 'id'
           AND is_identity = 'YES'`, [schema, table_name]
      );
      if (identityCheck.rows.length > 0) {
        const maxId = await this._pool.query(
          `SELECT COALESCE(MAX(id), 0) + 1 as next_id FROM "${schema}"."${table_name}"`
        );
        if (maxId.rows[0]?.next_id > 1) {
          await this._pool.query(
            `ALTER TABLE "${schema}"."${table_name}" ALTER COLUMN id RESTART WITH ${maxId.rows[0].next_id}`
          );
        }
      }
    }
  }

  public async listDocs(): Promise<DocEntry[]> {
    const result = await this._pool.query(
      `SELECT schema_name FROM information_schema.schemata
       WHERE schema_name LIKE 'doc_%' ORDER BY schema_name`
    );
    return result.rows.map((row: any) => ({
      name: row.schema_name,
      size: 0,  // TODO: calculate from pg_total_relation_size
    }));
  }

  public async deleteDoc(docName: string, _deletePermanently?: boolean): Promise<void> {
    const schema = docNameToSchema(docName);
    await this._pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
    log.info('PgDocStorageManager: deleted schema %s', schema);
  }

  public async renameDoc(oldName: string, newName: string): Promise<void> {
    const oldSchema = docNameToSchema(oldName);
    const newSchema = docNameToSchema(newName);
    await this._pool.query(`ALTER SCHEMA "${oldSchema}" RENAME TO "${newSchema}"`);
    log.info('PgDocStorageManager: renamed schema %s → %s', oldSchema, newSchema);
  }

  public async makeBackup(docName: string, backupTag: string): Promise<string> {
    // TODO: implement via pg_dump
    log.info('PgDocStorageManager: backup requested for %s (tag: %s) — not yet implemented', docName, backupTag);
    return `${docName}-${backupTag}`;
  }

  public async showItemInFolder(_docName: string): Promise<void> {
    // Not applicable for Postgres
  }

  public async closeStorage(): Promise<void> {
    // Don't close the shared pool
  }

  public async closeDocument(_docName: string): Promise<void> {
    // No per-document cleanup needed
  }

  public markAsChanged(_docName: string, _reason?: "edit"): void {
    // No-op for Postgres — no filesystem to track
  }

  public scheduleUsageUpdate(
    _docName: string, _usage: DocumentUsage | null, _minimizeDelay?: boolean
  ): void {
    // TODO: implement usage tracking
  }

  public testReopenStorage(): void {
    // No-op
  }

  public async addToStorage(_docName: string): Promise<void> {
    // No-op — schema already exists
  }

  public prepareToCloseStorage(): void {
    // No-op
  }

  public async getCopy(docName: string): Promise<string> {
    return this.exportToGristFile(docName);
  }

  /**
   * Export a Postgres-backed document to a temporary SQLite .grist file.
   * This is the inverse of importGristFile. Returns the path to the temp file.
   */
  public async exportToGristFile(docName: string): Promise<string> {
    const {PgDocStorage} = require('app/server/lib/PgDocStorage');
    const marshal = require('app/common/marshal');
    // @ts-ignore
    const sqlite3 = require('@gristlabs/sqlite3');
    const tmp = require('tmp');
    const schema = docNameToSchema(docName);

    // Create a temp SQLite file
    const tmpFile = tmp.fileSync({postfix: '.grist', keep: true});
    const tmpPath = tmpFile.name;

    // Open SQLite and create the schema
    const db = await new Promise<any>((resolve, reject) => {
      const d = new sqlite3.Database(tmpPath, (err: any) =>
        err ? reject(err) : resolve(d));
    });
    const runSql = (sql: string, params?: any[]): Promise<void> => new Promise((resolve, reject) =>
      db.run(sql, params || [], (err: any) => err ? reject(err) : resolve()));

    // Open PgDocStorage for reading
    const storage = new PgDocStorage(this, docName, this._pool);
    await storage.openFile();

    // Get the schema version
    const versionRow = await storage.get(`SELECT version FROM "_gristsys_version" WHERE id = 0`);
    const version = versionRow?.version ?? 0;
    await runSql(`PRAGMA user_version = ${version}`);

    // Get all tables in the Postgres schema
    const pgTables = await this._pool.query(
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = $1 AND table_type = 'BASE TABLE'
       AND table_name != '_gristsys_version'
       ORDER BY table_name`, [schema]
    );

    for (const {table_name: tableId} of pgTables.rows) {
      // Get column info from Postgres
      const pgCols = await this._pool.query(
        `SELECT column_name, data_type FROM information_schema.columns
         WHERE table_schema = $1 AND table_name = $2
         ORDER BY ordinal_position`, [schema, tableId]
      );

      // Build SQLite DDL — map Postgres types back to SQLite types
      const colDefs: string[] = [];
      const dataColNames: string[] = [];
      for (const {column_name: colName, data_type: pgType} of pgCols.rows) {
        if (colName === 'id') { continue; }
        if (colName.startsWith('gristAlt_')) { continue; }  // skip alt columns
        dataColNames.push(colName);
        // Map Postgres types to SQLite types
        let sqliteType = 'BLOB';
        if (pgType === 'text') { sqliteType = 'TEXT'; }
        else if (pgType === 'integer') { sqliteType = 'INTEGER'; }
        else if (pgType === 'numeric') { sqliteType = 'NUMERIC'; }
        else if (pgType === 'boolean') { sqliteType = 'BOOLEAN'; }
        else if (pgType === 'date') { sqliteType = 'DATE'; }
        else if (pgType === 'timestamp with time zone') { sqliteType = 'DATETIME'; }
        else if (pgType === 'bytea') { sqliteType = 'BLOB'; }
        else if (pgType === 'ARRAY') { sqliteType = 'TEXT'; }
        colDefs.push(`"${colName}" ${sqliteType}`);
      }

      const colDefSql = colDefs.length ? ', ' + colDefs.join(', ') : '';
      await runSql(`CREATE TABLE "${tableId}" (id INTEGER PRIMARY KEY${colDefSql})`);

      // Copy data — read from Postgres via fetchTable (which merges alt columns)
      const buf = await storage.fetchTable(tableId);
      const data = storage.decodeMarshalledData(buf, tableId);
      const ids = data.id || [];
      if (ids.length === 0) { continue; }

      // Insert rows
      const cols = dataColNames.filter(c => data[c] !== undefined);
      const placeholders = cols.map(() => '?').join(', ');
      const insertSql = `INSERT INTO "${tableId}" (${cols.map(c => `"${c}"`).join(', ')}, id) VALUES (${placeholders}, ?)`;

      for (let i = 0; i < ids.length; i++) {
        const params = cols.map(c => {
          const val = data[c][i];
          if (val === null || val === undefined) { return null; }
          // Marshal complex values (lists, objects) to blobs for SQLite
          if (Array.isArray(val) || (typeof val === 'object' && !(val instanceof Buffer) && !(val instanceof Date))) {
            const m = new marshal.Marshaller({version: 2});
            m.marshal(val);
            return Buffer.from(m.dump());
          }
          if (val === true) { return 1; }
          if (val === false) { return 0; }
          if (val === Infinity) { return 1e999; }
          if (val === -Infinity) { return -1e999; }
          return val;
        });
        params.push(ids[i]);
        await runSql(insertSql, params);
      }
    }

    await new Promise<void>((resolve, reject) =>
      db.close((err: any) => err ? reject(err) : resolve()));
    await storage.shutdown();

    log.info('PgDocStorageManager: exported %s to %s', schema, tmpPath);
    return tmpPath;
  }

  public async flushDoc(_docName: string): Promise<void> {
    // No-op — Postgres commits are durable
  }

  public async getSnapshots(_docName: string, _skipMetadataCache?: boolean): Promise<DocSnapshots> {
    return {snapshots: []};  // TODO: implement
  }

  public async removeSnapshots(_docName: string, _snapshotIds: string[]): Promise<void> {
    // TODO: implement
  }

  public getSnapshotProgress(_docName: string): SnapshotProgress {
    return new EmptySnapshotProgress();
  }

  public async replace(docName: string, options: any): Promise<void> {
    const sourceDocId = options.sourceDocId || docName;
    if (sourceDocId === docName && !options.snapshotId) { return; }
    if (options.snapshotId) {
      throw new Error('Snapshots not yet supported for Postgres backend');
    }
    // Drop the target and copy from source
    const destSchema = docNameToSchema(docName);
    await this._pool.query(`DROP SCHEMA IF EXISTS "${destSchema}" CASCADE`);
    await this.prepareFork(sourceDocId, docName);
  }

  public async getFsFileSize(_docName: string): Promise<number> {
    // TODO: calculate from pg_total_relation_size
    return 0;
  }
}
