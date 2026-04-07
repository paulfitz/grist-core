/**
 * PgMinDB - Postgres adapter implementing the MinDB interface.
 *
 * Translates SQLite-flavored SQL to Postgres, handles parameter binding
 * style differences, PRAGMA interception, and implements allMarshal()
 * in pure JS using the Marshaller class.
 */

// @ts-ignore
import {Pool, PoolClient, types as pgTypes} from 'pg';
import {Marshaller} from 'app/common/marshal';
import log from 'app/server/lib/log';

// Fix node-postgres returning NUMERIC/FLOAT as strings.
// These OIDs correspond to Postgres numeric types.
const NUMERIC_OID = 1700;
const FLOAT4_OID = 700;
const FLOAT8_OID = 701;
const INT8_OID = 20;
const DATE_OID = 1082;

pgTypes.setTypeParser(NUMERIC_OID, (val: string) => {
  const n = parseFloat(val);
  return isNaN(n) ? val : n;
});
pgTypes.setTypeParser(FLOAT4_OID, parseFloat);
pgTypes.setTypeParser(FLOAT8_OID, parseFloat);
pgTypes.setTypeParser(INT8_OID, (val: string) => {
  const n = parseInt(val, 10);
  return isNaN(n) ? val : n;
});
// Parse DATE as UTC to avoid timezone shifts. The default parser uses
// local time (new Date(y,m,d)), so in UTC-5 a date becomes epoch+18000.
pgTypes.setTypeParser(DATE_OID, (val: string) => {
  // val is 'YYYY-MM-DD'. Appending 'T00:00:00Z' forces UTC parsing.
  return new Date(val + 'T00:00:00Z');
});
import {
  MinDB,
  MinDBOptions,
  MinRunResult,
  PreparedStatement,
  ResultRow,
} from 'app/server/lib/SqliteCommon';

/**
 * Translate SQLite `?` parameter placeholders to Postgres `$1, $2, ...`.
 * Skips `?` inside single-quoted strings and double-quoted identifiers.
 */
function translateParams(sql: string): string {
  let out = '';
  let paramIdx = 0;
  let inSingle = false;
  let inDouble = false;
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];
    if (inSingle) {
      out += ch;
      if (ch === "'" && sql[i + 1] === "'") {
        out += "'";
        i++;  // skip escaped quote
      } else if (ch === "'") {
        inSingle = false;
      }
    } else if (inDouble) {
      out += ch;
      if (ch === '"' && sql[i + 1] === '"') {
        out += '"';
        i++;
      } else if (ch === '"') {
        inDouble = false;
      }
    } else if (ch === "'") {
      inSingle = true;
      out += ch;
    } else if (ch === '"') {
      inDouble = true;
      out += ch;
    } else if (ch === '?') {
      paramIdx++;
      out += '$' + paramIdx;
    } else {
      out += ch;
    }
  }
  return out;
}

/**
 * Fix boolean parameters: SQLite uses 1/0, node-postgres sends true/false
 * which may not work with integer columns.
 */
function fixParams(params: any[]): any[] {
  return params.map(p => p === true ? 1 : (p === false ? 0 : p));
}

/**
 * Apply common SQL dialect transformations from SQLite to Postgres.
 */
function translateSql(sql: string): string {
  // Translate sqlite_master references
  let result = translateSqliteMaster(sql);

  // Translate parameter placeholders
  result = translateParams(result);

  // INSERT OR REPLACE → INSERT ... ON CONFLICT (id) DO UPDATE SET ...
  // This is a rough translation; works for simple cases.
  const replaceMatch = result.match(
    /^INSERT\s+OR\s+REPLACE\s+INTO\s+("[^"]+"|[^\s(]+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i
  );
  if (replaceMatch) {
    const [, table, cols, vals] = replaceMatch;
    const colList = cols.split(',').map(c => c.trim());
    const setClauses = colList
      .filter(c => c !== 'id')
      .map(c => `${c} = EXCLUDED.${c}`)
      .join(', ');
    result = `INSERT INTO ${table} (${cols}) VALUES (${vals}) ON CONFLICT (id) DO UPDATE SET ${setClauses}`;
  }

  // INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
  result = result.replace(/INSERT\s+OR\s+IGNORE\s+INTO/gi, 'INSERT INTO');
  if (/ON\s+CONFLICT\s+DO\s+NOTHING/i.test(result) === false &&
      sql.match(/INSERT\s+OR\s+IGNORE/i)) {
    result = result.replace(
      /VALUES\s*\(([^)]+)\)/i,
      (match) => match + ' ON CONFLICT DO NOTHING'
    );
  }

  // IFNULL → COALESCE
  result = result.replace(/\bIFNULL\s*\(/gi, 'COALESCE(');

  // IS NOT <expr> → IS DISTINCT FROM <expr> (SQLite null-safe inequality)
  // Must not match IS NOT NULL (which is valid Postgres)
  // Handles both bare parameters ($N) and CAST expressions (CAST($N AS type))
  result = result.replace(/\bIS\s+NOT\s+(CAST\s*\([^)]+\)|\$\d+)/gi, 'IS DISTINCT FROM $1');

  // Type translations for DDL compatibility — only for DDL statements.
  // Must not replace column NAMES like "Blob" or "DateTime", only SQL type keywords.
  // The lookbehind (?<!") is safe because all DDL in the Postgres backend uses quoteIdent()
  // which always double-quotes identifiers. The DDL-only gate limits this to CREATE/ALTER.
  if (/^\s*(CREATE|ALTER)\b/i.test(result)) {
    // BLOB DEFAULT <value> → BYTEA DEFAULT NULL (Postgres won't accept int/string defaults for bytea)
    result = result.replace(/\bBLOB\s+DEFAULT\s+(?:0|''|"")/gi, 'BYTEA DEFAULT NULL');
    // Match BLOB or DATETIME preceded by a space/comma/paren (type position) not by a quote.
    result = result.replace(/(?<!")\bBLOB\b(?!")/gi, 'BYTEA');
    result = result.replace(/(?<!")\bDATETIME\b(?!")/gi, 'TIMESTAMPTZ');
  }
  // BOOLEAN DEFAULT 0 → BOOLEAN DEFAULT FALSE (Postgres won't accept integer defaults for boolean)
  result = result.replace(/\bBOOLEAN\s+DEFAULT\s+0\b/gi, 'BOOLEAN DEFAULT FALSE');
  // INTEGER PRIMARY KEY → INTEGER PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY
  // (SQLite auto-assigns rowid for INTEGER PRIMARY KEY; Postgres needs IDENTITY)
  result = result.replace(
    /\bINTEGER\s+PRIMARY\s+KEY\b(?!\s+(?:CHECK|GENERATED))/gi,
    'INTEGER PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY'
  );
  // 1e999 → 'Infinity' (SQLite uses 1e999 as Infinity; Postgres numeric supports 'Infinity')
  result = result.replace(/\b1e999\b/g, "'Infinity'");
  // Idempotent DDL for migrations that may encounter already-existing objects.
  result = result.replace(/\bADD\s+COLUMN\b(?!\s+IF)/gi, 'ADD COLUMN IF NOT EXISTS');
  result = result.replace(/\bCREATE TABLE\b(?!\s+IF)/gi, 'CREATE TABLE IF NOT EXISTS');
  // Quote mixed-case identifiers in DDL (Postgres lowercases unquoted identifiers).
  // Matches camelCase (storageId), _underscored_Mixed (_gristsys_Files), etc.
  if (/^\s*(CREATE|ALTER)\b/i.test(result)) {
    result = result.replace(/(?<!")(\b[a-z_]\w*[A-Z]\w*)\b(?!")/g, (_m, id) => `"${id}"`);
  }

  // COLLATE NOCASE (SQLite) → case-insensitive comparison for Postgres.
  // Simply remove the collation — the query code uses LOWER() or = for matching.
  result = result.replace(/\s+COLLATE\s+NOCASE\b/gi, '');

  return result;
}

/**
 * Check if SQL is a PRAGMA statement and handle it.
 * Returns {handled: true, result: ...} if handled, {handled: false} otherwise.
 */
function handlePragma(sql: string): {handled: boolean; result?: any} {
  const trimmed = sql.trim();
  if (!trimmed.toUpperCase().startsWith('PRAGMA')) {
    return {handled: false};
  }

  // PRAGMA user_version = N → handled via _gristsys_version table
  const setVersion = trimmed.match(/PRAGMA\s+user_version\s*=\s*(\d+)/i);
  if (setVersion) {
    return {handled: true, result: {type: 'set_version', version: parseInt(setVersion[1], 10)}};
  }

  // PRAGMA user_version → read from _gristsys_version table
  if (/PRAGMA\s+user_version\s*$/i.test(trimmed)) {
    return {handled: true, result: {type: 'get_version'}};
  }

  // PRAGMA table_info("tableName") → query information_schema
  const tableInfoMatch = trimmed.match(/PRAGMA\s+table_info\s*\(\s*"?([^)"]+)"?\s*\)/i);
  if (tableInfoMatch) {
    return {handled: true, result: {type: 'table_info', tableName: tableInfoMatch[1]}};
  }

  // All other PRAGMAs → no-op
  return {handled: true, result: {type: 'noop'}};
}

/**
 * Translate sqlite_master references to information_schema equivalents.
 */
function translateSqliteMaster(sql: string): string {
  // SELECT name FROM sqlite_master WHERE type='table'
  // → SELECT table_name as name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'
  return sql.replace(
    /SELECT\s+(\*|name)\s+FROM\s+sqlite_master\s+WHERE\s+type\s*=\s*'table'/gi,
    (match, cols) => {
      if (cols === '*') {
        return `SELECT table_name as name, 'table' as type FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'`;
      }
      return `SELECT table_name as name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'`;
    }
  );
}

/**
 * Convert Postgres-native values back to Grist's expected types.
 * - Date objects (from TIMESTAMPTZ) → epoch seconds (number)
 * - Booleans pass through (Grist engine expects true/false)
 */
function convertPgRow(row: any): any {
  for (const key of Object.keys(row)) {
    const val = row[key];
    if (val instanceof Date) {
      row[key] = val.getTime() / 1000;
    }
  }
  return row;
}

export class PgMinDB implements MinDB {
  private _schema: string;
  private _pool: Pool;
  // Dedicated connection for this document (not from pool)
  // Ensures SET search_path persists across queries
  private _dedicatedClient: PoolClient | null = null;
  // For transaction support: when inside a transaction, use this client
  private _txClient: PoolClient | null = null;
  // Track whether GRIST_DOC_SQL has been applied (to skip duplicate runs)
  private _gristDocSqlApplied: boolean = false;

  constructor(pool: Pool, schema: string) {
    this._pool = pool;
    this._schema = schema;
  }

  /**
   * Get or create a dedicated connection for this document.
   * Using a dedicated connection ensures SET search_path persists.
   */
  private async _getClient(): Promise<PoolClient> {
    if (!this._dedicatedClient) {
      const client = await this._pool.connect();
      try {
        await client.query(`SET search_path TO "${this._schema}"`);
      } catch (e) {
        client.release();
        throw e;
      }
      this._dedicatedClient = client;
    }
    return this._dedicatedClient;
  }

  public get schema(): string { return this._schema; }

  private _setVersionSql(): string {
    return `INSERT INTO "${this._schema}"._gristsys_version (id, version) VALUES (0, $1)
           ON CONFLICT (id) DO UPDATE SET version = $1`;
  }

  private _getVersionSql(): string {
    return `SELECT version FROM "${this._schema}"._gristsys_version WHERE id = 0`;
  }

  /**
   * Set the transaction client. While set, all queries use this client.
   */
  public setTxClient(client: PoolClient | null): void {
    this._txClient = client;
  }

  public hasTxClient(): boolean {
    return this._txClient !== null;
  }

  private _createIfNotExists: boolean = false;
  public setCreateIfNotExists(val: boolean): void {
    this._createIfNotExists = val;
  }

  private async _query(sql: string, params?: any[]): Promise<any> {
    const client = this._txClient || await this._getClient();
    try {
      return await client.query(sql, params);
    } catch (err: any) {
      const sqlPreview = sql.slice(0, 200);
      if (!err.message?.includes('current transaction is aborted')) {
        log.warn('PgMinDB query error: %s\n  SQL: %s', err.message, sql.slice(0, 200));
      }
      err.message = `${err.message}\n  SQL: ${sqlPreview}`;
      throw err;
    }
  }

  /**
   * Execute one or more SQL statements (no params, no results).
   */
  public async exec(sql: string): Promise<void> {
    // Detect GRIST_DOC_SQL / GRIST_DOC_WITH_TABLE1_SQL by the PRAGMA preamble.
    // If PgDocStorage.createFile() already ran these, skip to avoid duplicates.
    // Otherwise (called from _createDocFile), apply identifier quoting and run.
    let isGristDocSql = false;
    if (sql.trimStart().startsWith('PRAGMA foreign_keys=OFF')) {
      if (this._gristDocSqlApplied) {
        return;
      }
      this._gristDocSqlApplied = true;
      isGristDocSql = true;
      // Quote unquoted _grist* identifiers, strip PRAGMA/BEGIN/COMMIT wrappers,
      // and quote camelCase column names in CREATE INDEX.
      sql = sql
        .replace(/PRAGMA\s+foreign_keys\s*=\s*\w+/gi, '')
        .replace(/BEGIN TRANSACTION/gi, '')
        .replace(/\bCOMMIT\b/gi, '')
        .replace(/(?<!")(_grist\w+)(?!")/g, (_m: string, name: string) => `"${name}"`)
        .replace(/\((\w*[A-Z]\w*)\)/g, (_m: string, col: string) => `("${col}")`)
        // Metadata tables use BOOLEAN but INSERT VALUES use literal 0/1.
        // Convert BOOLEAN to INTEGER so literal values work.
        .replace(/\bBOOLEAN\b/gi, 'INTEGER')
        // Make INSERTs idempotent so re-creating an existing schema doesn't fail.
        .replace(/\bINSERT\s+INTO\b/gi, 'INSERT OR IGNORE INTO')
        // Make CREATE INDEX idempotent for re-creation.
        .replace(/\bCREATE INDEX\b/gi, 'CREATE INDEX IF NOT EXISTS');
    }

    // Handle PRAGMA
    const pragma = handlePragma(sql);
    if (pragma.handled) {
      if (pragma.result?.type === 'set_version') {
        await this._query(this._setVersionSql(), [pragma.result.version]);
      } else if (pragma.result?.type === 'get_version') {
        // get_version via exec doesn't return anything; use get() for that
      }
      // noop for others
      return;
    }

    // Split on semicolons for multi-statement support.
    const statements = splitStatements(sql);
    for (const stmt of statements) {
      const trimmed = stmt.trim();
      if (!trimmed) continue;
      const p = handlePragma(trimmed);
      if (p.handled) {
        if (p.result?.type === 'set_version') {
          await this._query(
            `INSERT INTO "${this._schema}"._gristsys_version (id, version) VALUES (0, $1)
             ON CONFLICT (id) DO UPDATE SET version = $1`,
            [p.result.version]
          );
        }
        continue;
      }
      try {
        let translated = translateSql(trimmed);
        if (this._createIfNotExists) {
          translated = translated
            .replace(/\bCREATE TABLE\b(?!\s+IF)/gi, 'CREATE TABLE IF NOT EXISTS')
            .replace(/\bCREATE UNIQUE INDEX\b(?!\s+IF)/gi, 'CREATE UNIQUE INDEX IF NOT EXISTS')
            .replace(/\bCREATE INDEX\b(?!\s+IF)/gi, 'CREATE INDEX IF NOT EXISTS');
        }
        await this._query(translated);
        // After creating a user table in GRIST_DOC_SQL, add alt companion columns
        // using the catalog (not regex). This handles Table1 from initial SQL.
        if (isGristDocSql) {
          const createMatch = translated.match(
            /CREATE TABLE\s+(?:IF NOT EXISTS\s+)?"([^"]+)"/i
          );
          if (createMatch) {
            if (!createMatch[1].startsWith('_grist')) {
              await this._addAltColumnsViaAlter(createMatch[1]);
            } else {
              // For metadata tables, fix ChoiceList/RefList columns: TEXT→BYTEA.
              // These columns store marshalled binary which contains null bytes
              // that Postgres TEXT rejects.
              await this._fixListColumnsToByteaViaAlter(createMatch[1]);
            }
          }
        }
      } catch (e: any) {
        e.message = `PgMinDB exec error: ${e.message}\nSQL: ${trimmed.slice(0, 200)}`;
        throw e;
      }
    }
  }

  /**
   * For metadata tables created via GRIST_DOC_SQL, change ChoiceList/RefList columns
   * from TEXT to BYTEA. These columns store marshalled binary with null bytes that
   * Postgres TEXT rejects.
   */
  private async _fixListColumnsToByteaViaAlter(tableName: string): Promise<void> {
    const gristSchema = require('app/common/schema').schema;
    const colTypes: Record<string, string> = gristSchema[tableName] || {};
    const alterClauses: string[] = [];
    for (const [colName, colType] of Object.entries(colTypes)) {
      const base = colType.split(':')[0];
      if (base === 'ChoiceList' || base === 'RefList' || base === 'Attachments') {
        alterClauses.push(`ALTER COLUMN "${colName}" TYPE BYTEA USING NULL`);
      }
    }
    if (alterClauses.length > 0) {
      await this._query(
        `ALTER TABLE "${this._schema}"."${tableName}" ${alterClauses.join(', ')}`
      );
    }
  }

  /**
   * After creating a user table via GRIST_DOC_SQL, add gristAlt_ bytea companion
   * columns by querying the catalog for actual columns. This avoids fragile regex
   * parsing of CREATE TABLE statements.
   */
  private async _addAltColumnsViaAlter(tableName: string): Promise<void> {
    const result = await this._query(
      `SELECT column_name FROM information_schema.columns
       WHERE table_schema = $1 AND table_name = $2
       ORDER BY ordinal_position`,
      [this._schema, tableName]
    );
    const addClauses: string[] = [];
    for (const row of result.rows) {
      const colName: string = row.column_name;
      if (colName === 'id' || colName === 'manualSort') continue;
      if (colName.startsWith('gristAlt_')) continue;
      addClauses.push(`ADD COLUMN IF NOT EXISTS "gristAlt_${colName}" BYTEA DEFAULT NULL`);
    }
    if (addClauses.length > 0) {
      await this._query(
        `ALTER TABLE "${this._schema}"."${tableName}" ${addClauses.join(', ')}`
      );
    }
  }

  /**
   * Run a single SQL statement with parameters. Returns row count.
   */
  public async run(sql: string, ...params: any[]): Promise<MinRunResult> {
    const pragma = handlePragma(sql);
    if (pragma.handled) {
      if (pragma.result?.type === 'set_version') {
        await this._query(this._setVersionSql(), [pragma.result.version]);
      }
      return {changes: 0};
    }
    const translated = translateSql(sql);
    const result = await this._query(
      translated,
      fixParams(params)
    );
    return {changes: result.rowCount || 0};
  }

  /**
   * Execute a query and return the first row, or undefined.
   */
  public async get(sql: string, ...params: any[]): Promise<ResultRow | undefined> {
    const pragma = handlePragma(sql);
    if (pragma.handled) {
      if (pragma.result?.type === 'get_version') {
        const r = await this._query(this._getVersionSql());
        if (r.rows.length > 0) {
          return {user_version: r.rows[0].version};
        }
        return {user_version: 0};
      }
      return undefined;
    }
    const translated = translateSql(sql);
    const result = await this._query(
      translated,
      fixParams(params)
    );
    const row = result.rows[0];
    return row ? convertPgRow(row) : undefined;
  }

  /**
   * Execute a query and return all rows.
   */
  public async all(sql: string, ...params: any[]): Promise<ResultRow[]> {
    const pragma = handlePragma(sql);
    if (pragma.handled) {
      if (pragma.result?.type === 'get_version') {
        const r = await this._query(this._getVersionSql());
        return r.rows.map((row: any) => ({user_version: row.version}));
      }
      if (pragma.result?.type === 'table_info') {
        // Return rows matching SQLite's PRAGMA table_info format:
        // {cid, name, type, notnull, dflt_value, pk}
        const r = await this._query(
          `SELECT ordinal_position - 1 as cid,
                  column_name as name,
                  UPPER(data_type) as type,
                  CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END as notnull,
                  column_default as dflt_value,
                  0 as pk
           FROM information_schema.columns
           WHERE table_schema = $1 AND table_name = $2
           ORDER BY ordinal_position`,
          [this._schema, pragma.result.tableName]
        );
        // Mark the 'id' column as primary key, filter out gristAlt_ companions
        const filtered = r.rows.filter((row: any) => !row.name.startsWith('gristAlt_'));
        for (const row of filtered) {
          if (row.name === 'id') { row.pk = 1; }
        }
        return filtered;
      }
      return [];
    }
    const translated = translateSql(sql);
    const result = await this._query(
      translated,
      fixParams(params)
    );
    return result.rows.map(convertPgRow);
  }

  /**
   * Execute a query and return results in Grist marshal format.
   * This reimplements the native C++ allMarshal from the sqlite3 fork.
   *
   * Returns a Buffer containing a marshalled dict: {colName: [val1, val2, ...], ...}
   */
  public async allMarshal(sql: string, ...params: any[]): Promise<Buffer> {
    return this._allMarshalImpl(sql, params);
  }

  // Accepts params as an array directly (avoids stack overflow with 100k+ filter values).
  public async allMarshalArray(sql: string, params: any[]): Promise<Buffer> {
    return this._allMarshalImpl(sql, params);
  }

  private async _allMarshalImpl(sql: string, params: any[]): Promise<Buffer> {
    const pragma = handlePragma(sql);
    if (pragma.handled) {
      const marshaller = new Marshaller({version: 2, keysAreBuffers: true});
      marshaller.marshal({});
      return marshaller.dumpAsBuffer();
    }

    const translated = translateSql(sql);
    const result = await this._query(
      translated,
      params.length > 0 ? fixParams(params) : undefined
    );

    // Convert Postgres-native types (Date→epoch, etc.) and build column-oriented structure
    const rows = result.rows.map(convertPgRow);
    const columns: Record<string, any[]> = {};
    const fields = result.fields || [];
    for (const field of fields) {
      columns[field.name] = rows.map((r: any) => r[field.name]);
    }

    // Marshal using Grist's format (version 2, keys as buffers)
    const marshaller = new Marshaller({version: 2, keysAreBuffers: true});
    marshaller.marshal(columns);
    return marshaller.dumpAsBuffer();
  }

  /**
   * Prepare a statement. Returns a minimal PreparedStatement.
   */
  public async prepare(sql: string): Promise<PreparedStatement> {
    const translated = translateSql(sql);
    let columnNames: string[] = [];

    const self = this;
    return {
      async run(...params: any[]): Promise<MinRunResult> {
        const result = await self._query(translated, fixParams(params));
        return {changes: result.rowCount || 0};
      },
      async finalize(): Promise<void> {
        // nothing to do
      },
      columns(): string[] {
        return columnNames;
      },
    };
  }

  /**
   * Run an INSERT and return the new row's id.
   * Appends RETURNING id to the statement.
   */
  public async runAndGetId(sql: string, ...params: any[]): Promise<number> {
    const translated = translateSql(sql);
    // Add RETURNING id if not already present
    let finalSql = translated;
    if (!/RETURNING\s+/i.test(finalSql)) {
      finalSql = finalSql.replace(/;?\s*$/, ' RETURNING id');
    }
    const result = await this._query(finalSql, fixParams(params));
    if (result.rows && result.rows.length > 0) {
      return result.rows[0].id;
    }
    return 0;
  }

  public async close(): Promise<void> {
    if (this._dedicatedClient) {
      this._dedicatedClient.release();
      this._dedicatedClient = null;
    }
  }

  public async limitAttach(_maxAttach: number): Promise<void> {
    // no-op for Postgres
  }

  public async interrupt(): Promise<void> {
    // Cancel the current query on the dedicated client
    if (this._dedicatedClient) {
      const pid = (this._dedicatedClient as any).processID;
      if (pid) {
        try {
          await this._pool.query(`SELECT pg_cancel_backend($1)`, [pid]);
        } catch { /* best effort */ }
      }
    }
  }

  public getOptions(): MinDBOptions {
    return {
      canInterrupt: true,
      bindableMethodsProcessOneStatement: true,
    };
  }
}

/**
 * Split SQL text on semicolons, respecting quoted strings.
 */
function splitStatements(sql: string): string[] {
  const statements: string[] = [];
  let current = '';
  let inSingle = false;
  let inDouble = false;

  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];
    if (inSingle) {
      current += ch;
      if (ch === "'" && sql[i + 1] === "'") {
        current += "'";
        i++;
      } else if (ch === "'") {
        inSingle = false;
      }
    } else if (inDouble) {
      current += ch;
      if (ch === '"' && sql[i + 1] === '"') {
        current += '"';
        i++;
      } else if (ch === '"') {
        inDouble = false;
      }
    } else if (ch === "'") {
      inSingle = true;
      current += ch;
    } else if (ch === '"') {
      inDouble = true;
      current += ch;
    } else if (ch === ';') {
      statements.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  if (current.trim()) {
    statements.push(current);
  }
  return statements;
}
