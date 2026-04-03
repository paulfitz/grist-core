/**
 * PgDocStorage - Postgres document storage backend for Grist.
 *
 * Implements the same public interface as DocStorage but stores data
 * in PostgreSQL instead of SQLite. Each document gets its own Postgres
 * schema.
 *
 * For MVP, all user data columns are the same SQL types as in SQLite
 * (TEXT, INTEGER, NUMERIC, etc.). Values are stored as primitives where
 * possible. Since Postgres has strict typing, non-conforming values
 * (which SQLite would accept due to loose typing) are stored as NULL
 * for now — a full solution requires the column-pair model.
 */

import chunk from 'lodash/chunk';
import * as _ from 'underscore';
// @ts-ignore
import {Pool} from 'pg';

import * as marshal from 'app/common/marshal';
import {DocAction} from 'app/common/DocActions';
import {BulkColValues, TableColValues, TableDataAction, toTableDataAction} from 'app/common/DocActions';
import * as gristTypes from 'app/common/gristTypes';
import {isList} from 'app/common/gristTypes';
import {combineExpr} from 'app/server/lib/ExpandedQuery';
import {ISQLiteDB, MigrationHooks, RunResult} from 'app/server/lib/SQLiteDB';
import {quoteIdent} from 'app/server/lib/SQLiteDB';
import {PreparedStatement, ResultRow} from 'app/server/lib/SqliteCommon';
import {IDocStorageManager} from 'app/server/lib/IDocStorageManager';
import {PgMinDB} from 'app/server/lib/PgMinDB';
import {DocStorage} from 'app/server/lib/DocStorage';
import * as schema from 'app/common/schema';
import log from 'app/server/lib/log';

const maxParameters = 500;

// Native Postgres type for a Grist column type (used for user data columns).
function getNativePgType(colType: string | null): string {
  switch (colType) {
    case 'Bool': return 'boolean';
    case 'Choice':
    case 'Text': return 'text';
    case 'ChoiceList': return 'text[]';
    case 'RefList':
    case 'ReferenceList':
    case 'Attachments': return 'integer[]';
    case 'Date': return 'date';
    case 'DateTime': return 'timestamptz';
    case 'Int':
    case 'Id':
    case 'Ref':
    case 'Reference': return 'integer';
    case 'Numeric':
    case 'ManualSortPos':
    case 'PositionNumber': return 'numeric';
  }
  if (colType) {
    if (colType.startsWith('Ref:')) return 'integer';
    if (colType.startsWith('RefList:')) return 'integer[]';
    if (colType.startsWith('DateTime:')) return 'timestamptz';
  }
  return 'jsonb';
}

// SQLite-compatible SQL type (used for metadata tables that go through PgMinDB translations).
function getSqlType(colType: string | null): string {
  switch (colType) {
    case 'Bool': return 'BOOLEAN';
    case 'Choice':
    case 'Text': return 'TEXT';
    case 'ChoiceList':
    case 'RefList':
    case 'ReferenceList':
    case 'Attachments': return 'TEXT';
    case 'Date': return 'DATE';
    case 'DateTime': return 'DATETIME';
    case 'Int':
    case 'Id':
    case 'Ref':
    case 'Reference': return 'INTEGER';
    case 'Numeric':
    case 'ManualSortPos':
    case 'PositionNumber': return 'NUMERIC';
  }
  if (colType) {
    if (colType.startsWith('Ref:')) return 'INTEGER';
    if (colType.startsWith('RefList:')) return 'TEXT';
    if (colType.startsWith('DateTime:')) return 'DATETIME';
  }
  return 'BLOB';
}

function formattedDefault(colType: string): string {
  const def = gristTypes.getDefaultForType(colType, {sqlFormatted: true});
  if (def === '1e999') return '1e308';
  return String(def ?? 'NULL');
}

// Column definition for metadata tables (goes through PgMinDB type translations).
function columnDef(colId: string, colType: string): string {
  return `${quoteIdent(colId)} ${getSqlType(colType)} DEFAULT ${formattedDefault(colType)}`;
}

// Alt column name for the escape hatch companion.
function altColName(colId: string): string {
  return `gristAlt_${colId}`;
}

// Whether a column needs an alt companion (user data columns only, not id/manualSort).
function needsAltColumn(colId: string, tableId: string): boolean {
  return colId !== 'id' && colId !== 'manualSort' && !tableId.startsWith('_grist');
}

// Native column default for Postgres. Most types default to NULL.
function nativeDefault(colType: string): string {
  const base = colType.split(':')[0];
  switch (base) {
    case 'Text': case 'Choice': return "DEFAULT ''";
    case 'Bool': return 'DEFAULT FALSE';
    case 'Int': case 'Ref': case 'Reference': case 'Id': return 'DEFAULT 0';
    case 'Numeric': return 'DEFAULT 0';
    default: return 'DEFAULT NULL';
  }
}

// DDL for a user data column (native type + alt companion).
function userColumnDefs(colId: string, colType: string): string {
  const nativeDef = `${quoteIdent(colId)} ${getNativePgType(colType)} ${nativeDefault(colType)}`;
  const altDef = `${quoteIdent(altColName(colId))} jsonb DEFAULT NULL`;
  return `${nativeDef}, ${altDef}`;
}

// Convert a Grist value to the native Postgres type, or null if non-conforming.
function encodeNativeValue(gristType: string, val: any): any {
  if (val === null || val === undefined) return null;
  const base = gristType.split(':')[0];

  switch (base) {
    case 'Bool':
      if (val === true || val === false) return val;
      if (val === 1) return true;
      if (val === 0) return false;
      return null;  // non-conforming

    case 'Int': case 'Ref': case 'Reference': case 'Id':
      if (typeof val === 'number' && isFinite(val)) return Math.round(val);
      return null;

    case 'Numeric': case 'ManualSortPos': case 'PositionNumber':
      if (typeof val === 'number') return isFinite(val) ? val : null;
      return null;

    case 'Text': case 'Choice':
      if (typeof val === 'string') return val;
      return null;

    case 'Date':
      // Grist stores dates as epoch seconds. Convert to ISO date string
      // which Postgres accepts for the date type.
      if (typeof val === 'number' && isFinite(val) && val !== 0) {
        const d = new Date(val * 1000);
        return d.toISOString().slice(0, 10);  // 'YYYY-MM-DD'
      }
      if (val === 0) return null;  // Grist uses 0 for empty dates
      return null;

    case 'DateTime':
      if (typeof val === 'number' && isFinite(val) && val !== 0) {
        return new Date(val * 1000);  // node-postgres handles Date→timestamptz
      }
      if (val === 0) return null;
      return null;

    case 'ChoiceList':
      if (isList(val) && val.slice(1).every((v: any) => typeof v === 'string')) {
        return val.slice(1);  // ['L', 'a', 'b'] → ['a', 'b'] (node-postgres → text[])
      }
      return null;

    case 'RefList':
      if (isList(val) && val.slice(1).every((v: any) => typeof v === 'number')) {
        return val.slice(1);  // ['L', 1, 2] → [1, 2] (node-postgres → integer[])
      }
      return null;

    default:
      // Any, Attachments, etc. — store as jsonb.
      // node-postgres needs JSON-stringified values for jsonb columns.
      return JSON.stringify(val);
  }
}

// Convert a Grist value to jsonb for the alt companion. Only called for non-conforming values.
function encodeAltValue(val: any): any {
  if (val === null || val === undefined) return null;
  return JSON.stringify(val);  // jsonb accepts any JSON
}

function prefixJoin(prefix: string, items: string[]): string {
  return items.length ? prefix + items.join(prefix) : '';
}

// Quote a table identifier for Postgres.
function quoteTable(tableId: string): string {
  return quoteIdent(tableId);
}

function isMetadataTable(tableId: string): boolean {
  return tableId.startsWith('_grist_');
}

// Decode a value read from Postgres back to Grist format.
// For user data: the native column value is used if non-NULL, otherwise
// the alt companion value is used. This function handles the native value.
function decodeNativeValue(val: any, gristType: string): any {
  if (val === null || val === undefined) return null;
  const base = gristType.split(':')[0];

  switch (base) {
    case 'Bool':
      // node-postgres returns boolean; Grist engine expects boolean
      return val;

    case 'Date':
      // node-postgres returns Date; Grist expects epoch seconds
      // convertPgRow already handles Date→epoch, so this is a no-op
      return val;

    case 'DateTime':
      // Same — convertPgRow handles Date→epoch
      return val;

    case 'ChoiceList':
      // node-postgres returns text[]; Grist expects ['L', 'a', 'b']
      if (Array.isArray(val)) return ['L', ...val];
      return val;

    case 'RefList':
      // node-postgres returns integer[]; Grist expects ['L', 1, 2]
      if (Array.isArray(val)) return ['L', ...val];
      return val;

    default:
      return val;
  }
}

// Decode an alt companion value (jsonb) back to Grist format.
function decodeAltValue(val: any): any {
  // jsonb values come back as parsed JS objects from node-postgres
  return val;
}

// Encode columns to rows for metadata tables (old path, no column pairs).
function encodeColumnsToRowsSimple(types: string[], valueColumns: any[][]): any[][] {
  const rows = _.unzip(valueColumns);
  for (const row of rows) {
    for (let i = 0; i < row.length; i++) {
      const val = row[i];
      if (val === null || val === undefined) { row[i] = null; continue; }
      if (typeof val === 'boolean') { row[i] = val ? 1 : 0; continue; }
      if (typeof val === 'number' && !isFinite(val)) { row[i] = val > 0 ? 1e308 : -1e308; continue; }
      // For metadata tables, marshal complex types
      if (Array.isArray(val) || val instanceof Uint8Array || Buffer.isBuffer(val)) {
        const marshaller = new marshal.Marshaller({version: 2});
        marshaller.marshal(val);
        const buf = marshaller.dump();
        row[i] = '\\x' + Buffer.from(buf).toString('hex');
        continue;
      }
      // Primitives pass through
    }
  }
  return rows;
}

// Sanitize a document name for use as a Postgres schema name.
function docNameToSchema(docName: string): string {
  return 'doc_' + docName.replace(/[^a-zA-Z0-9_]/g, '_').slice(0, 60).toLowerCase();
}

// ExpandedQuery type (matches DocStorage's usage)
interface ExpandedQuery {
  tableId: string;
  filters: {[colId: string]: any[]};
  limit?: number;
  where?: {clause: string; params: any[]};
  joins?: string[];
  selects?: string[];
}

export class PgDocStorage implements ISQLiteDB {
  public docPath: string;
  public docName: string;
  public storageManager: IDocStorageManager;

  private _db: PgMinDB;
  private _pool: Pool;
  private _schema: string;
  private _initialized: boolean = false;
  private _docSchema: {[tableId: string]: {[colId: string]: string}} = {};
  private _txPromise: Promise<any> = Promise.resolve();

  // Marker for MigrationUtils to detect Postgres backend
  public readonly _isPgBackend = true;

  constructor(storageManager: IDocStorageManager, docName: string, pgPool: Pool) {
    this.storageManager = storageManager;
    this.docName = docName;
    this.docPath = docName;  // virtual path
    this._pool = pgPool;
    this._schema = docNameToSchema(docName);
    this._db = new PgMinDB(pgPool, this._schema);
    this._docSchema = Object.assign({}, schema.schema);
  }

  // ── Lifecycle ───────────────────────────────────────────────────

  public async openFile(_hooks: MigrationHooks = {}): Promise<void> {
    // Verify schema exists
    const result = await this._pool.query(
      `SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1`,
      [this._schema]
    );
    if (result.rows.length === 0) {
      throw new Error(`Document schema ${this._schema} does not exist`);
    }
    this._initialized = true;
    await this._updateMetadata();
  }

  public async createFile(_options?: {useExisting?: boolean}): Promise<void> {
    // Create schema
    await this._pool.query(`CREATE SCHEMA IF NOT EXISTS "${this._schema}"`);

    // Create _gristsys_version table (Postgres equivalent of PRAGMA user_version)
    await this.exec(`CREATE TABLE IF NOT EXISTS "_gristsys_version" (
      id INTEGER PRIMARY KEY CHECK (id = 0), version INTEGER DEFAULT 0)`);
    await this.exec(`INSERT INTO "_gristsys_version" VALUES(0, 9) ON CONFLICT DO NOTHING`);

    // Create system tables, adding IF NOT EXISTS to handle reopens/copies
    this._db.setCreateIfNotExists(true);
    try {
      await DocStorage.docStorageSchema.create(this as any);
    } finally {
      this._db.setCreateIfNotExists(false);
    }

    // The _grist_* metadata tables are created by ActiveDoc._createDocFile()
    // which runs GRIST_DOC_SQL or GRIST_DOC_WITH_TABLE1_SQL through exec().
    // PgMinDB detects these by the PRAGMA preamble, applies identifier quoting,
    // and handles the type translations.

    this._initialized = true;
  }

  public isInitialized(): boolean {
    return this._initialized;
  }

  public async shutdown(): Promise<void> {
    this._initialized = false;
    await this._db.close();  // Release dedicated connection back to pool
  }

  // ── ISQLiteDB interface ─────────────────────────────────────────

  public async exec(sql: string): Promise<void> {
    return this._db.exec(sql);
  }

  public async run(sql: string, ...params: any[]): Promise<RunResult> {
    const result = await this._db.run(sql, ...params);
    return {changes: result.changes} as RunResult;
  }

  public async get(sql: string, ...params: any[]): Promise<ResultRow | undefined> {
    return this._db.get(sql, ...params);
  }

  public async all(sql: string, ...params: any[]): Promise<ResultRow[]> {
    return this._db.all(sql, ...params);
  }

  public async prepare(sql: string): Promise<PreparedStatement> {
    return this._db.prepare(sql);
  }

  public async execTransaction<T>(callback: () => Promise<T>): Promise<T> {
    // Support nesting: if we're already in a transaction, just run the callback
    if (this._db.hasTxClient()) {
      return callback();
    }
    // Serialize transactions (same as SQLite — one at a time)
    return this._txPromise = this._txPromise.catch((err) => {
      log.warn('PgDocStorage: previous transaction failed: %s', err?.message);
    }).then(async () => {
      const client = await this._pool.connect();
      try {
        await client.query('BEGIN');
        await client.query(`SET search_path TO "${this._schema}"`);
        this._db.setTxClient(client);
        const result = await callback();
        await client.query('COMMIT');
        return result;
      } catch (err) {
        await client.query('ROLLBACK');
        throw err;
      } finally {
        this._db.setTxClient(null);
        client.release();
      }
    });
  }

  public async runAndGetId(sql: string, ...params: any[]): Promise<number> {
    return this._db.runAndGetId(sql, ...params);
  }

  public async requestVacuum(): Promise<boolean> {
    // TODO: VACUUM for Postgres (requires being outside a transaction)
    return false;
  }

  // ── Data fetching ───────────────────────────────────────────────

  public fetchTable(tableId: string): Promise<Buffer> {
    return this.fetchQuery({tableId, filters: {}});
  }

  public async getNextRowId(tableId: string): Promise<number> {
    const row = await this.get(`SELECT MAX(id) as "maxId" FROM ${quoteTable(tableId)}`);
    if (!row) {
      throw new Error(`Error in PgDocStorage.getNextRowId: no table ${tableId}`);
    }
    return row.maxId ? row.maxId + 1 : 1;
  }

  public getColumnType(tableId: string, colId: string): string | undefined {
    return this._docSchema[tableId]?.[colId];
  }

  public async fetchActionData(
    tableId: string, rowIds: number[], colIds?: string[]
  ): Promise<TableDataAction> {
    const colSpec = colIds ? ['id', ...colIds].map(c => quoteIdent(c)).join(', ') : '*';
    let fullValues: TableColValues | undefined;

    for (const rowIdChunk of chunk(rowIds, maxParameters)) {
      const sqlArg = rowIdChunk.map((_, i) => `$${i + 1}`).join(',');
      // Need to use raw pg query since allMarshal goes through the ? translator
      const marshalled: Buffer = await this._db.allMarshal(
        `SELECT ${colSpec} FROM ${quoteTable(tableId)} WHERE id IN (${sqlArg})`,
        ...rowIdChunk
      );
      const colValues: TableColValues = this.decodeMarshalledData(marshalled, tableId);
      if (!fullValues) {
        fullValues = colValues;
      } else {
        for (const col of Object.keys(colValues)) {
          fullValues[col].push(...colValues[col]);
        }
      }
    }
    return toTableDataAction(tableId, fullValues || {id: []});
  }

  public async fetchQuery(query: ExpandedQuery): Promise<Buffer> {
    const params: any[] = query.where?.params || [];
    const whereParts: string[] = [];
    for (const colId of Object.keys(query.filters)) {
      const values = query.filters[colId];
      if (values.length === 0) {
        whereParts.push('FALSE');  // Postgres doesn't support IN ()
      } else {
        whereParts.push(
          `${quoteTable(query.tableId)}.${quoteIdent(colId)} IN (${values.map(() => '?').join(', ')})`
        );
        params.push(...values);
      }
    }
    const sql = this._getSqlForQuery(query, whereParts);
    return this._db.allMarshal(sql, ...params);
  }

  public async getAllTableNames(): Promise<string[]> {
    const rows = await this._pool.query(
      `SELECT table_name as name FROM information_schema.tables
       WHERE table_schema = $1 AND table_type = 'BASE TABLE'`,
      [this._schema]
    );
    return rows.rows.map((r: any) => r.name);
  }

  // ── Marshalling / decoding ──────────────────────────────────────

  public decodeMarshalledData(
    marshalledData: Buffer | Uint8Array, tableId: string
  ): TableColValues {
    const columnValues: TableColValues = marshal.loads(marshalledData);

    if (isMetadataTable(tableId)) {
      // Metadata tables: simple decode (no column pairs)
      for (const col of Object.keys(columnValues)) {
        const type = this._getGristType(tableId, col);
        const column = columnValues[col];
        for (let i = 0; i < column.length; i++) {
          column[i] = decodeNativeValue(column[i], type);
        }
      }
      return columnValues;
    }

    // User data tables: merge native + alt columns
    const ALT_PREFIX = 'gristAlt_';
    const altCols = new Set<string>();

    for (const col of Object.keys(columnValues)) {
      if (col.startsWith(ALT_PREFIX)) {
        altCols.add(col);
      }
    }

    // Merge native + alt columns
    const result: TableColValues = {} as TableColValues;
    for (const col of Object.keys(columnValues)) {
      if (altCols.has(col)) { continue; }

      const altKey = ALT_PREFIX + col;
      const type = this._getGristType(tableId, col);
      const nativeColumn = columnValues[col];

      if (altCols.has(altKey)) {
        const altColumn = columnValues[altKey];
        const merged: any[] = [];
        for (let i = 0; i < nativeColumn.length; i++) {
          const altVal = altColumn[i];
          if (altVal !== null && altVal !== undefined) {
            merged.push(decodeAltValue(altVal));
          } else {
            merged.push(decodeNativeValue(nativeColumn[i], type));
          }
        }
        result[col] = merged;
      } else {
        result[col] = nativeColumn;
      }
    }

    return result;
  }

  public decodeMarshalledDataFromTables(
    marshalledData: Buffer | Uint8Array
  ): BulkColValues {
    const columnValues: BulkColValues = marshal.loads(marshalledData);
    for (const col of Object.keys(columnValues)) {
      const [tableId, colId] = col.split('.');
      const type = this._getGristType(tableId, colId);
      const column = columnValues[col];
      for (let i = 0; i < column.length; i++) {
        column[i] = decodeNativeValue(column[i], type);
      }
    }
    return columnValues;
  }

  // ── Action application ──────────────────────────────────────────

  public async applyStoredActions(docActions: DocAction[]): Promise<void> {
    docActions = this._compressStoredActions(docActions);
    for (const action of docActions) {
      try {
        await this.applyStoredAction(action);
      } catch (e: any) {
        if (String(e).match(/column.*manualSort.*does not exist|no column named manualSort/i)) {
          const modifiedAction = this._considerWithoutManualSort(action);
          if (modifiedAction) {
            await this.applyStoredAction(modifiedAction);
            return;
          }
        }
        throw e;
      }
    }
  }

  public async applyStoredAction(action: DocAction): Promise<void> {
    const actionType = action[0];
    const f = (this as any)['_process_' + actionType];
    if (typeof f !== 'function') {
      log.error('Unknown action: ' + actionType);
    } else {
      await f.apply(this, action.slice(1));
      const tableId = action[1];
      if (isMetadataTable(tableId) && actionType !== 'AddTable') {
        await this._updateMetadata();
      }
    }
  }

  // ── DocAction processors ────────────────────────────────────────

  public async _process_AddTable(tableId: string, columns: any[]): Promise<void> {
    if (isMetadataTable(tableId)) {
      // Metadata tables use SQLite-compatible types (PgMinDB translates them)
      const colSpecSql = prefixJoin(', ',
        columns.map((c: any) => columnDef(c.id, c.type)));
      await this.exec(`CREATE TABLE ${quoteTable(tableId)} (id INTEGER PRIMARY KEY${colSpecSql})`);
    } else {
      // User data tables use native Postgres types + alt companions
      const colSpecs: string[] = [];
      for (const c of columns) {
        if (c.id === 'manualSort') {
          colSpecs.push(`${quoteIdent(c.id)} numeric DEFAULT 1e308`);
        } else {
          colSpecs.push(userColumnDefs(c.id, c.type));
        }
      }
      const colSpecSql = colSpecs.length ? ', ' + colSpecs.join(', ') : '';
      await this.exec(`CREATE TABLE ${quoteTable(tableId)} (id INTEGER PRIMARY KEY${colSpecSql})`);
    }
  }

  public _process_UpdateRecord(tableId: string, rowId: string, columnValues: any): Promise<void> {
    return this._process_BulkUpdateRecord(tableId, [rowId],
      _.mapObject(columnValues, (val: any) => [val]));
  }

  public _process_AddRecord(tableId: string, rowId: number, columnValues: any): Promise<void> {
    return this._process_BulkAddRecord(tableId, [rowId],
      _.mapObject(columnValues, (val: any) => [val]));
  }

  public async _process_BulkUpdateRecord(
    tableId: string, rowIds: string[], columnValues: any
  ): Promise<void> {
    const cols = Object.keys(columnValues);
    if (!rowIds.length || !cols.length) return;

    if (isMetadataTable(tableId)) {
      const colListSql = cols.map(c => quoteIdent(c) + '=?').join(', ');
      const sql = `UPDATE ${quoteTable(tableId)} SET ${colListSql} WHERE id=?`;
      const types = cols.map(c => this._getGristType(tableId, c));
      const sqlParams = encodeColumnsToRowsSimple(types,
        cols.map(c => columnValues[c]).concat([rowIds]));
      await this._applyBulkSql(sql, sqlParams);
    } else {
      // User data: dual-write native + alt columns
      const setClauses: string[] = [];
      for (const c of cols) {
        if (needsAltColumn(c, tableId)) {
          setClauses.push(`${quoteIdent(c)}=?, ${quoteIdent(altColName(c))}=?::jsonb`);
        } else {
          setClauses.push(`${quoteIdent(c)}=?`);
        }
      }
      const sql = `UPDATE ${quoteTable(tableId)} SET ${setClauses.join(', ')} WHERE id=?`;
      const sqlParams = this._encodeUserRows(tableId, cols, columnValues, rowIds);
      await this._applyBulkSql(sql, sqlParams);
    }
  }

  public async _process_BulkAddRecord(
    tableId: string, rowIds: number[], columnValues: {[key: string]: any}
  ): Promise<void> {
    if (rowIds.length === 0) return;

    const cols = Object.keys(columnValues);

    if (isMetadataTable(tableId)) {
      const colListSql = cols.map(c => quoteIdent(c) + ', ').join('');
      const placeholders = cols.map(() => '?, ').join('');
      const sql = `INSERT INTO ${quoteTable(tableId)} (${colListSql}id) VALUES (${placeholders}?)`;
      const types = cols.map(c => this._getGristType(tableId, c));
      const sqlParams = encodeColumnsToRowsSimple(types,
        cols.map(c => columnValues[c]).concat([rowIds]));
      await this._applyBulkSql(sql, sqlParams);
    } else {
      // User data: dual-write native + alt columns
      const colParts: string[] = [];
      const phParts: string[] = [];
      for (const c of cols) {
        if (needsAltColumn(c, tableId)) {
          colParts.push(`${quoteIdent(c)}, ${quoteIdent(altColName(c))}`);
          phParts.push('?, ?::jsonb');
        } else {
          colParts.push(quoteIdent(c));
          phParts.push('?');
        }
      }
      const sql = `INSERT INTO ${quoteTable(tableId)} (${colParts.join(', ')}, id) VALUES (${phParts.join(', ')}, ?)`;
      const sqlParams = this._encodeUserRows(tableId, cols, columnValues, rowIds);
      await this._applyBulkSql(sql, sqlParams);
    }
  }

  public async _process_RemoveRecord(tableId: string, rowId: string): Promise<RunResult> {
    return this.run(`DELETE FROM ${quoteTable(tableId)} WHERE id=?`, rowId);
  }

  public async _process_ReplaceTableData(
    tableId: string, rowIds: number[], columnValues: any[]
  ): Promise<void> {
    await this.exec('DELETE FROM ' + quoteTable(tableId));
    await this._process_BulkAddRecord(tableId, rowIds, columnValues);
  }

  public async _process_BulkRemoveRecord(tableId: string, rowIds: number[]): Promise<void> {
    if (rowIds.length === 0) return;
    for (const idChunk of chunk(rowIds, maxParameters)) {
      const placeholders = idChunk.map(() => '?').join(',');
      await this.run(
        `DELETE FROM ${quoteTable(tableId)} WHERE id IN (${placeholders})`,
        ...idChunk
      );
    }
  }

  public async _process_AddColumn(tableId: string, colId: string, colInfo: any): Promise<void> {
    if (isMetadataTable(tableId)) {
      await this.exec(
        `ALTER TABLE ${quoteTable(tableId)} ADD COLUMN ${columnDef(colId, colInfo.type)}`
      );
    } else {
      // Add native column + alt companion
      const nativeDef = `${quoteIdent(colId)} ${getNativePgType(colInfo.type)} ${nativeDefault(colInfo.type)}`;
      const altDef = `${quoteIdent(altColName(colId))} jsonb DEFAULT NULL`;
      await this.exec(
        `ALTER TABLE ${quoteTable(tableId)} ADD COLUMN ${nativeDef}, ADD COLUMN ${altDef}`
      );
    }
  }

  public async _process_RenameColumn(
    tableId: string, fromColId: string, toColId: string
  ): Promise<void> {
    if (fromColId === 'id' || fromColId === 'manualSort' || tableId.startsWith('_grist')) {
      throw new Error('Cannot rename internal Grist column');
    }
    await this.exec(
      `ALTER TABLE ${quoteTable(tableId)} RENAME COLUMN ${quoteIdent(fromColId)} TO ${quoteIdent(toColId)}`
    );
    // Also rename alt companion if it exists
    if (needsAltColumn(fromColId, tableId)) {
      await this.exec(
        `ALTER TABLE ${quoteTable(tableId)} RENAME COLUMN ${quoteIdent(altColName(fromColId))} TO ${quoteIdent(altColName(toColId))}`
      );
    }
  }

  public async _process_ModifyColumn(
    tableId: string, colId: string, colInfo: any
  ): Promise<void> {
    if (!colInfo) {
      log.error('ModifyColumn action called without params.');
      return;
    }
    if (colInfo.type) {
      if (isMetadataTable(tableId)) {
        const newSqlType = getSqlType(colInfo.type);
        await this.exec(
          `ALTER TABLE ${quoteTable(tableId)} ALTER COLUMN ${quoteIdent(colId)} TYPE ${newSqlType} USING ${quoteIdent(colId)}::${newSqlType}`
        );
      } else {
        // Change native column type; clear values and let them be re-written
        const newNativeType = getNativePgType(colInfo.type);
        await this.exec(
          `ALTER TABLE ${quoteTable(tableId)} ALTER COLUMN ${quoteIdent(colId)} TYPE ${newNativeType} USING NULL`
        );
      }
    }
  }

  public async _process_RemoveColumn(tableId: string, colId: string): Promise<void> {
    await this.exec(
      `ALTER TABLE ${quoteTable(tableId)} DROP COLUMN IF EXISTS ${quoteIdent(colId)}`
    );
    // Also drop alt companion if it exists
    if (needsAltColumn(colId, tableId)) {
      await this.exec(
        `ALTER TABLE ${quoteTable(tableId)} DROP COLUMN IF EXISTS ${quoteIdent(altColName(colId))}`
      );
    }
  }

  public async _process_RenameTable(fromTableId: string, toTableId: string): Promise<void> {
    if (fromTableId === toTableId) return;
    if (fromTableId.toLowerCase() === toTableId.toLowerCase()) {
      const tmpId = '_tmp_rename_' + fromTableId;
      await this.exec(`ALTER TABLE ${quoteTable(fromTableId)} RENAME TO ${quoteTable(tmpId)}`);
      await this.exec(`ALTER TABLE ${quoteTable(tmpId)} RENAME TO ${quoteTable(toTableId)}`);
    } else {
      await this.exec(
        `ALTER TABLE ${quoteTable(fromTableId)} RENAME TO ${quoteTable(toTableId)}`
      );
    }
  }

  public async _process_RemoveTable(tableId: string): Promise<void> {
    await this.exec(`DROP TABLE IF EXISTS ${quoteTable(tableId)}`);
  }

  // ── Attachment stubs ────────────────────────────────────────────

  public async attachFileIfNew(
    _fileIdent: string, _fileData?: Buffer, _storageId?: string
  ): Promise<boolean> {
    // TODO: implement
    return false;
  }

  public async attachOrUpdateFile(
    _fileIdent: string, _fileData?: Buffer, _storageId?: string
  ): Promise<boolean> {
    return false;
  }

  public async getFileInfo(_fileIdent: string): Promise<any> {
    return null;
  }

  public async getFileInfoNoData(_fileIdent: string): Promise<any> {
    return null;
  }

  public async listAllFiles(): Promise<any[]> {
    return [];
  }

  public async removeUnusedAttachments(): Promise<void> {}

  public async scanAttachmentsForUsageChanges(): Promise<any[]> {
    return [];
  }

  public async findAttachmentReferences(_attId: number): Promise<any[]> {
    return [];
  }

  public async getSoftDeletedAttachmentIds(_expiredOnly: boolean): Promise<number[]> {
    return [];
  }

  public async getTotalAttachmentFileSizes(): Promise<number> {
    return 0;
  }

  // ── Plugin data stubs ───────────────────────────────────────────

  public async getPluginDataItem(_pluginId: string, _key: string): Promise<any> {
    return undefined;
  }

  public async hasPluginDataItem(_pluginId: string, _key: string): Promise<boolean> {
    return false;
  }

  public async setPluginDataItem(
    _pluginId: string, _key: string, _value: string
  ): Promise<void> {}

  public async removePluginDataItem(_pluginId: string, _key: string): Promise<void> {}

  public async clearPluginDataItem(_pluginId: string): Promise<void> {}

  // ── Other stubs and utilities ───────────────────────────────────

  public async renameDocTo(_newName: string): Promise<void> {
    // TODO: rename schema
  }

  public async getDataSize(): Promise<number> {
    return 0;  // TODO
  }

  public async getDataSizeUncached(): Promise<number> {
    return 0;
  }

  public async vacuum(): Promise<void> {}

  public async updateIndexes(_desiredIndexes: any[]): Promise<void> {}

  public async testGetIndexes(): Promise<any[]> {
    return [];
  }

  public async interrupt(): Promise<void> {}

  public getOptions(): any {
    return this._db.getOptions();
  }

  public getDB(): any {
    return this._db;
  }

  // Static method matching DocStorage
  public static decodeRowValues(dbRow: ResultRow): any {
    for (const key of Object.keys(dbRow)) {
      const val = dbRow[key];
      if (val instanceof Uint8Array || Buffer.isBuffer(val)) {
        dbRow[key] = marshal.loads(val);
      }
    }
    return dbRow;
  }

  // ── Private helpers ─────────────────────────────────────────────

  private async _updateMetadata(): Promise<void> {
    try {
      const rows = await this.all(
        'SELECT t."tableId", c."colId", c."type" ' +
        'FROM "_grist_Tables_column" c JOIN "_grist_Tables" t ON c."parentId"=t.id'
      );
      const s: {[key: string]: any} = {};
      for (const {tableId, colId, type} of rows) {
        const table = s.hasOwnProperty(tableId) ? s[tableId] : (s[tableId] = {});
        table[colId] = type;
      }
      this._docSchema = Object.assign(s, schema.schema);
    } catch (err: any) {
      if (err.message?.includes('does not exist') ||
          err.message?.includes('relation') && err.message?.includes('does not exist')) {
        err.message = `NO_METADATA_ERROR: ${this.docName} has no metadata`;
        if (!err.cause) { err.cause = {}; }
        (err.cause as any).code = 'NO_METADATA_ERROR';
      }
      throw err;
    }
  }

  private _getGristType(tableId: string, colId: string): string {
    return (this._docSchema[tableId]?.[colId]) || 'Any';
  }

  private _getSqlForQuery(query: ExpandedQuery, whereParts: string[]): string {
    const whereCondition = combineExpr('AND', [query.where?.clause, ...whereParts]);
    const whereClause = whereCondition ? `WHERE ${whereCondition}` : '';
    const limitClause = (typeof query.limit === 'number') ? `LIMIT ${query.limit}` : '';
    const joinClauses = query.joins ? query.joins.join(' ') : '';
    const selects = query.selects ? query.selects.join(', ') : '*';
    // Postgres doesn't guarantee row order; always add ORDER BY id for consistency
    const orderClause = query.joins ? '' : 'ORDER BY id';
    return `SELECT ${selects} FROM ${quoteTable(query.tableId)} ${joinClauses} ${whereClause} ${orderClause} ${limitClause}`;
  }

  // Encode user data rows with native + alt column pairs.
  // For each column, if the value conforms to the native type, write it there
  // and set alt to NULL. Otherwise write NULL to native and the raw value to alt.
  private _encodeUserRows(
    tableId: string, cols: string[], columnValues: any, rowIds: any[]
  ): any[][] {
    const numRows = rowIds.length;
    const rows: any[][] = [];
    for (let r = 0; r < numRows; r++) {
      const row: any[] = [];
      for (const c of cols) {
        const gristType = this._getGristType(tableId, c);
        const val = columnValues[c][r];
        if (needsAltColumn(c, tableId)) {
          const nativeVal = encodeNativeValue(gristType, val);
          if (nativeVal !== null || val === null || val === undefined) {
            // Value conforms (or is null) — write native, alt is NULL
            row.push(nativeVal);
            row.push(null);
          } else {
            // Non-conforming — write NULL native, raw value to alt
            row.push(null);
            row.push(encodeAltValue(val));
          }
        } else {
          // manualSort or similar — single column, simple encode
          row.push(val);
        }
      }
      row.push(rowIds[r]);
      rows.push(row);
    }
    return rows;
  }

  private async _applyBulkSql(sql: string, sqlParams: any[][]): Promise<void> {
    if (sqlParams.length === 1) {
      await this.run(sql, ...sqlParams[0]);
    } else {
      const stmt = await this.prepare(sql);
      for (const params of sqlParams) {
        await stmt.run(...params);
      }
      await stmt.finalize();
    }
  }

  // Compress AddRecord + UpdateRecord sequences (same as DocStorage)
  private _compressStoredActions(docActions: DocAction[]): DocAction[] {
    if (docActions.length > 1) {
      const first = docActions[0];
      if (first[0] === 'AddRecord' &&
        docActions.slice(1).every(
          a => a[0] === 'UpdateRecord' && a[1] === first[1] && a[2] === first[2]
        )) {
        const merged = JSON.parse(JSON.stringify(first));
        for (const a2 of docActions.slice(1)) {
          Object.assign(merged[3], a2[3]);
        }
        return [merged];
      }
    }
    return docActions;
  }

  // Try action without manualSort column (same as DocStorage)
  private _considerWithoutManualSort(action: DocAction): DocAction | null {
    if (action[0] === 'BulkAddRecord' || action[0] === 'ReplaceTableData') {
      const cols = action[3] as {[key: string]: any};
      if ('manualSort' in cols) {
        const newCols = {...cols};
        delete newCols.manualSort;
        return [action[0], action[1], action[2], newCols] as DocAction;
      }
    } else if (action[0] === 'AddRecord') {
      const cols = action[3] as {[key: string]: any};
      if ('manualSort' in cols) {
        const newCols = {...cols};
        delete newCols.manualSort;
        return ['AddRecord', action[1], action[2], newCols] as DocAction;
      }
    }
    return null;
  }
}
