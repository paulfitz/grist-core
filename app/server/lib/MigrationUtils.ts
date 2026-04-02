/**
 * Backend-agnostic helpers for storage migrations.
 *
 * Migrations use plain SQL for operations that work identically on SQLite
 * and Postgres (CREATE TABLE, ALTER TABLE ADD COLUMN, INSERT, CREATE INDEX).
 * These helpers cover the operations that differ between backends:
 *
 *   - listTables: sqlite_master vs information_schema
 *   - listColumns: PRAGMA table_info vs information_schema
 *   - rebuildTable: table rebuild with column transforms (SQLite uses the
 *     classic create-tmp/copy/drop/rename dance; Postgres uses ALTER COLUMN)
 */

import {DocStorage} from 'app/server/lib/DocStorage';
import {ISQLiteDB, quoteIdent} from 'app/server/lib/SQLiteDB';
import {ResultRow} from 'app/server/lib/SqliteCommon';

function _isPg(db: ISQLiteDB): boolean {
  return Boolean((db as any)._isPgBackend);
}

/**
 * List all table names in the database.
 */
export async function listTables(db: ISQLiteDB): Promise<string[]> {
  if (_isPg(db)) {
    const rows = await db.all(
      `SELECT table_name as name FROM information_schema.tables
       WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'`
    );
    return rows.map(r => r.name);
  } else {
    const rows = await db.all("SELECT name FROM sqlite_master WHERE type='table'");
    return rows.map(r => r.name);
  }
}

/**
 * List columns for a table. Returns ResultRows with at least {name, type, dflt_value}.
 * On SQLite these come from PRAGMA table_info; on Postgres from information_schema.
 */
export async function listColumns(db: ISQLiteDB, tableId: string): Promise<ResultRow[]> {
  if (_isPg(db)) {
    const rows = await db.all(
      `SELECT column_name as name, UPPER(data_type) as type, column_default as dflt_value
       FROM information_schema.columns
       WHERE table_schema = current_schema() AND table_name = $1
       ORDER BY ordinal_position`,
      tableId
    );
    return rows;
  } else {
    return db.all(`PRAGMA table_info(${quoteIdent(tableId)})`);
  }
}

/**
 * Rebuild a table with column transformations. Each column's info row
 * (from listColumns/PRAGMA table_info) is passed through the transform,
 * which can modify type/default or return {info, valueSql} to also
 * transform data during copy.
 *
 * On SQLite: uses the same create-tmp/copy/drop/rename dance as the
 * original DocStorage migrations, with DocStorage._sqlColSpecFromDBInfo
 * for column formatting (preserving exact schema output).
 *
 * On Postgres: emits individual ALTER COLUMN / UPDATE statements.
 */
export async function rebuildTable(
  db: ISQLiteDB, tableId: string,
  transform: (info: ResultRow) => ResultRow | {info: ResultRow; valueSql: string}
): Promise<void> {
  if (_isPg(db)) {
    await _pgRebuildTable(db, tableId, transform);
  } else {
    await _sqliteRebuildTable(db, tableId, transform);
  }
}

/**
 * Check if a table exists.
 */
export async function tableExists(db: ISQLiteDB, tableId: string): Promise<boolean> {
  const tables = await listTables(db);
  return tables.includes(tableId);
}

// SQLite: table rebuild using same pattern as original DocStorage migrations.
async function _sqliteRebuildTable(
  db: ISQLiteDB, tableId: string,
  transform: (info: ResultRow) => ResultRow | {info: ResultRow; valueSql: string}
): Promise<void> {
  // Get column info via PRAGMA (same as original migrations)
  const infoRows: ResultRow[] = await db.all(`PRAGMA table_info(${quoteIdent(tableId)})`);

  const results = infoRows.map(row => {
    if (row.name === 'id') {
      return {info: row, valueSql: 'id'};
    }
    const t = transform(row);
    if ('info' in t) {
      return t;
    }
    return {info: t, valueSql: quoteIdent(row.name)};
  });

  // Build column specs using DocStorage._sqlColSpecFromDBInfo (preserves exact format)
  const newColSpecSql = results.map(r =>
    r.info.name === 'id' ? 'id INTEGER PRIMARY KEY' : DocStorage._sqlColSpecFromDBInfo(r.info)
  ).join(', ');
  const valuesSql = results.map(r => r.valueSql).join(', ');

  // Check if anything actually changed
  const origColSpecSql = infoRows.map(row =>
    row.name === 'id' ? 'id INTEGER PRIMARY KEY' : DocStorage._sqlColSpecFromDBInfo(row)
  ).join(', ');
  if (newColSpecSql === origColSpecSql) {
    return;  // Nothing changed
  }

  // Do the rebuild dance (same as original DocStorage migrations)
  const tmpTableId = DocStorage._makeTmpTableId(tableId);
  await db.exec(`CREATE TABLE ${quoteIdent(tmpTableId)} (${newColSpecSql})`);
  await db.exec(`INSERT INTO ${quoteIdent(tmpTableId)} SELECT ${valuesSql} FROM ${quoteIdent(tableId)}`);
  await db.exec(`DROP TABLE ${quoteIdent(tableId)}`);
  await db.exec(`ALTER TABLE ${quoteIdent(tmpTableId)} RENAME TO ${quoteIdent(tableId)}`);
}

// Postgres: use ALTER COLUMN for each changed column.
async function _pgRebuildTable(
  db: ISQLiteDB, tableId: string,
  transform: (info: ResultRow) => ResultRow | {info: ResultRow; valueSql: string}
): Promise<void> {
  const infoRows = await listColumns(db, tableId);

  for (const row of infoRows) {
    if (row.name === 'id') { continue; }
    const result = transform(row);
    const newInfo = 'info' in result ? result.info : result;

    if (newInfo.type !== row.type) {
      await db.exec(
        `ALTER TABLE ${quoteIdent(tableId)} ALTER COLUMN ${quoteIdent(newInfo.name)} ` +
        `TYPE ${newInfo.type} USING ${quoteIdent(newInfo.name)}::${newInfo.type}`
      );
    }
    if (newInfo.dflt_value !== row.dflt_value && newInfo.dflt_value != null) {
      await db.exec(
        `ALTER TABLE ${quoteIdent(tableId)} ALTER COLUMN ${quoteIdent(newInfo.name)} ` +
        `SET DEFAULT ${newInfo.dflt_value}`
      );
    }
    if ('valueSql' in result) {
      await db.exec(
        `UPDATE ${quoteIdent(tableId)} SET ${quoteIdent(newInfo.name)} = ${result.valueSql}`
      );
    }
  }
}
