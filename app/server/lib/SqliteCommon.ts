import { Marshaller } from "app/common/marshal";
import { OpenMode, quoteIdent, quotePlain } from "app/server/lib/SQLiteDB";

/**
 * Code common to SQLite wrappers.
 */

/**
 * It is important that Statement exists - but we don't expect
 * anything of it.
 */
export interface Statement {
}

export interface MinDB {
  exec(sql: string): Promise<void>;
  run(sql: string, ...params: any[]): Promise<MinRunResult>;
  get(sql: string, ...params: any[]): Promise<ResultRow|undefined>;
  all(sql: string, ...params: any[]): Promise<ResultRow[]>;
  prepare(sql: string, ...params: any[]): Promise<PreparedStatement>;
  runAndGetId(sql: string, ...params: any[]): Promise<number>;
  close(): Promise<void>;
  allMarshal(sql: string, ...params: any[]): Promise<Buffer>;

  /**
   * Limit the number of ATTACHed databases permitted.
   */
  limitAttach(maxAttach: number): Promise<void>;
}

export interface MinRunResult {
  changes: number;
}

// Describes the result of get() and all() database methods.
export interface ResultRow {
  [column: string]: any;
}

export interface PreparedStatement {
  run(...params: any[]): Promise<MinRunResult>;
  finalize(): Promise<void>;
  columns(): string[];
}

export interface SqliteVariant {
  opener(dbPath: string, mode: OpenMode): Promise<MinDB>;
}

/**
 * A crude implementation of Grist marshalling.
 * There is a fork of node-sqlite3 that has Grist
 * marshalling built in, at:
 *   https://github.com/gristlabs/node-sqlite3
 * If using a version of SQLite without this built
 * in, another option is to add custom functions
 * to do it. This object has the initialize, step,
 * and finalize callbacks typically needed to add
 * a custom aggregration function.
 */
export const gristMarshal = {
  initialize() {
    return {};
  },
  step(array: any, ...nextValue: any[]) {
    if (!array.names) {
      array.names = nextValue;
      array.maps = [];
      for (let i = 0; i < nextValue.length; i++) {
        array.maps[i] = [];
      }
    } else {
      for (const [i, v] of nextValue.entries()) {
        let vi = v;
        if (ArrayBuffer.isView(v)) {
          const vv = new Uint8Array(v.buffer);
          if (vv[0] === 0) {
            vi = '';
          }
        }
        array.maps[i].push(vi);
      }
    }
    return array;
  },
  finalize(array: any) {
    const final = new Marshaller({version: 2, keysAreBuffers: true});
    const result: any = {};
    for (const [i, name] of (array.names as string[]).entries()) {
      const mm = array.maps[i];
      result[name] = mm;
    }
    final.marshal(result);
    return final.dumpAsBuffer();
  }
};

/**
 * Run Grist marshalling as a SQLite query, assuming
 * a custom aggregation has been added as "grist_marshal".
 */
export async function allMarshalQuery(db: MinDB, sql: string, ...params: any[]): Promise<Buffer> {
  const q = await db.prepare(sql);
  const cols = q.columns();
  const qq = cols.map(quoteIdent).join(',');
  const names = cols.map((c: string) =>
    quotePlain(c) + ' as ' + quoteIdent(c)).join(',');
  const test = await db.all(`select grist_marshal(${qq}) as buf FROM ` +
    `(select ${names} UNION ALL select * from (` + sql + '))', ..._fixParameters(params));
  return test[0].buf;
}

/**
 * Booleans need to be cast to 1 or 0 for SQLite.
 * The node-sqlite3 wrapper does this automatically, but other
 * wrappers do not.
 */
function _fixParameters(params: any[]) {
  return params.map(p => p === true ? 1 : (p === false ? 0 : p));
}
