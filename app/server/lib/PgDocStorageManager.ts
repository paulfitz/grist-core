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
    // Return the schema name as a virtual "path"
    return docNameToSchema(docName);
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

  public async prepareToCreateDoc(_docName: string): Promise<void> {
    // No-op — PgDocStorage.createFile() creates the schema
  }

  /**
   * Import a .grist (SQLite) file into a Postgres schema. Creates the
   * schema using PgDocStorage.createFile() (which sets up metadata tables
   * with proper Postgres types), then reads data from SQLite and writes
   * it through PgDocStorage's DocAction methods (which handle native
   * types + gristAlt_ columns for user data).
   */
  public async importGristFile(docName: string, gristPath: string): Promise<void> {
    const {PgDocStorage} = require('app/server/lib/PgDocStorage');
    const marshal = require('app/common/marshal');
    // @ts-ignore
    const sqlite3 = require('@gristlabs/sqlite3');

    // Open SQLite
    const db = await new Promise<any>((resolve, reject) => {
      const d = new sqlite3.Database(gristPath, sqlite3.OPEN_READONLY, (err: any) =>
        err ? reject(err) : resolve(d));
    });
    const allSql = (sql: string): Promise<any[]> => new Promise((resolve, reject) =>
      db.all(sql, (err: any, rows: any[]) => err ? reject(err) : resolve(rows)));

    // Create the Postgres schema with proper structure via PgDocStorage
    const storage = new PgDocStorage(this, docName, this._pool);
    await storage.createFile();

    // Also create _grist_* metadata tables (normally done by _createDocFile)
    const {GRIST_DOC_SQL} = require('app/server/lib/initialDocSql');
    await storage.exec(GRIST_DOC_SQL);

    // Read Grist column type metadata from the SQLite file
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

    // Import each table's data
    const sqliteTables = await allSql(
      "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    );

    // Import metadata tables first so _docSchema is populated before user tables
    const metaTables = sqliteTables.filter((t: any) => t.name.startsWith('_grist_'));
    const userTables = sqliteTables.filter((t: any) => !t.name.startsWith('_grist') && !t.name.startsWith('_gristsys_'));
    const orderedTables = [...metaTables, ...userTables];

    for (const {name: tableId} of orderedTables) {
      const rows = await allSql(`SELECT * FROM "${tableId}"`);
      if (rows.length === 0 && !tableId.startsWith('_grist_')) { continue; }

      // Decode marshalled BLOB values from SQLite
      for (const row of rows) {
        for (const key of Object.keys(row)) {
          if (Buffer.isBuffer(row[key])) {
            try { row[key] = marshal.loads(row[key]); } catch { /* keep as-is */ }
          }
        }
      }

      // For user tables: create via AddTable DocAction (creates native + alt columns)
      if (!tableId.startsWith('_grist_')) {
        const colTypes = colTypeMap.get(tableId) || new Map<string, string>();
        const pragmaCols = await allSql(`PRAGMA table_info("${tableId}")`);
        const colSpecs = pragmaCols
          .filter((c: any) => c.name && c.name !== 'id')
          .map((c: any) => ({id: c.name, type: colTypes.get(c.name) || 'Any'}));
        try {
          await storage.applyStoredAction(['AddTable', tableId, colSpecs] as any);
        } catch { /* table may already exist (e.g., Table1 from GRIST_DOC_WITH_TABLE1_SQL) */ }
      }

      // Clear seed data and insert imported data
      try { await storage.exec(`DELETE FROM "${tableId}"`); } catch { /* ok */ }

      if (rows.length > 0) {
        const colIds = Object.keys(rows[0]).filter(c => c !== 'id');
        const rowIds = rows.map((r: any) => r.id);
        const colValues: any = {};
        for (const c of colIds) {
          colValues[c] = rows.map((r: any) => r[c]);
        }
        try {
          await storage.applyStoredAction(
            ['BulkAddRecord', tableId, rowIds, colValues] as any
          );
        } catch (e: any) {
          log.warn('PgDocStorageManager: import failed for %s: %s',
            tableId, e.message?.split('\n')[0]);
        }
      }
    }

    await new Promise<void>((resolve, reject) =>
      db.close((err: any) => err ? reject(err) : resolve()));
    await storage.shutdown();

    log.info('PgDocStorageManager: imported %s into schema %s', gristPath, docNameToSchema(docName));
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

    // Copy each table (structure + data)
    for (const {table_name} of tables.rows) {
      await this._pool.query(
        `CREATE TABLE "${destSchema}"."${table_name}" (LIKE "${srcSchema}"."${table_name}" INCLUDING ALL)`
      );
      await this._pool.query(
        `INSERT INTO "${destSchema}"."${table_name}" SELECT * FROM "${srcSchema}"."${table_name}"`
      );
    }

    return destDocName;
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
    // Create a temporary copy by forking to a temp name
    const copyName = `${docName}_copy_${Date.now()}`;
    await this.prepareFork(docName, copyName);
    return copyName;
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

  public async replace(_docName: string, _options: any): Promise<void> {
    // TODO: implement document replacement
    throw new Error('Document replacement not yet implemented for Postgres backend');
  }

  public async getFsFileSize(_docName: string): Promise<number> {
    // TODO: calculate from pg_total_relation_size
    return 0;
  }
}
