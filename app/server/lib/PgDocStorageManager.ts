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
