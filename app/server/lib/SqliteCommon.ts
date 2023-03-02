import { Marshaller } from "app/common/marshal";
import { OpenMode, quoteIdent, quotePlain } from "app/server/lib/SQLiteDB";

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


export interface Thing {
  lastInsertRowid: any;
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

export const gristMarshal = {
  initialize() {
    return {};
  },
  step(array: any, ...nextValue: any[]) {
    console.log('step', {array, nextValue});
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
    console.log("DID STEP");
    return array;
  },
  finalize(array: any) {
    console.log("FINALIZE", {array});
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

export async function allMarshalQuery(db: MinDB, sql: string, ...params: any[]): Promise<Buffer> {
  const q = await db.prepare(sql);
  const cols = q.columns();
  const qq = cols.map(quoteIdent).join(',');
  const names = cols.map((c: string) =>
    quotePlain(c) + ' as ' + quoteIdent(c)).join(',');
  console.log(`select grist_marshal(${qq}) as buf`);
  const test = await db.all(`select grist_marshal(${qq}) as buf FROM ` +
    `(select ${names} UNION ALL select * from (` + sql + '))', ...tweaked(params));
  return test[0].buf;
}

function tweaked(params: any[]) {
  return params.map(p => p === true ? 1 : (p === false ? 0 : p));
}

