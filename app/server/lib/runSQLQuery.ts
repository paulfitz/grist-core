import { ApiError } from "app/common/ApiError";
import * as Types from "app/plugin/DocApiTypes";
import { ActiveDoc } from "app/server/lib/ActiveDoc";
import { appSettings } from "app/server/lib/AppSettings";
import { docSessionFromRequest, OptDocSession } from "app/server/lib/DocSession";
import log from "app/server/lib/log";
import { optIntegerParam } from "app/server/lib/requestUtils";
import { isRequest, RequestOrSession } from "app/server/lib/sessionUtils";

// Maximum duration of a `runSQLQuery` call. Does not apply to internal calls to SQLite.
const MAX_CUSTOM_SQL_MSEC = appSettings
  .section("integrations")
  .section("sql")
  .flag("timeout")
  .requireInt({
    envVar: "GRIST_SQL_TIMEOUT_MSEC",
    defaultValue: 1000,
  });

/**
 * Executes a SQL SELECT statement on a document and returns the result.
 */
export async function runSQLQuery(
  requestOrSession: NonNullable<RequestOrSession>,
  activeDoc: ActiveDoc,
  options: Types.SqlPost,
) {
  let docSession: OptDocSession;
  if (isRequest(requestOrSession)) {
    docSession = docSessionFromRequest(requestOrSession);
  } else {
    docSession = requestOrSession;
  }
  if (!(await activeDoc.canCopyEverything(docSession))) {
    throw new ApiError("insufficient document access", 403);
  }

  const statement = options.sql.replace(/;$/, "");
  // A very loose test, just for early error message
  if (!statement.toLowerCase().includes("select")) {
    throw new ApiError("only select statements are supported", 400);
  }

  const sqlOptions = activeDoc.docStorage.getOptions();
  if (
    !sqlOptions?.canInterrupt ||
    !sqlOptions?.bindableMethodsProcessOneStatement
  ) {
    throw new ApiError("The available SQLite wrapper is not adequate", 500);
  }
  const timeout = Math.max(
    0,
    Math.min(
      MAX_CUSTOM_SQL_MSEC,
      optIntegerParam(options.timeout, "timeout") || MAX_CUSTOM_SQL_MSEC,
    ),
  );
  // Wrap in a select to commit to the SELECT branch of SQLite
  // grammar. Note ; isn't a problem.
  //
  // The underlying SQLite functions used will only process the
  // first statement in the supplied text. For node-sqlite3, the
  // remainder is placed in a "tail string" ignored by that library.
  // So a Robert'); DROP TABLE Students;-- style attack isn't applicable.
  //
  // Since Grist is used with multiple SQLite wrappers, not just
  // node-sqlite3, we have added a bindableMethodsProcessOneStatement
  // flag that will need adding for each wrapper, and this endpoint
  // will not operate unless that flag is set to true.
  //
  // The text is wrapped in select * from (USER SUPPLIED TEXT) which
  // puts SQLite unconditionally onto the SELECT branch of its
  // grammar. It is straightforward to break out of such a wrapper
  // with multiple statements, but again, only the first statement
  // is processed.
  // For Postgres, auto-quote table and column names that the user left unquoted.
  // Postgres lowercases unquoted identifiers, breaking references to CamelCase tables.
  let quotedStatement = statement;
  if (process.env.GRIST_DOC_BACKEND === 'postgres' && activeDoc.docData) {
    // Collect all known table and column names
    const knownNames = new Set<string>();
    const tables = activeDoc.docData.getMetaTable("_grist_Tables");
    for (const rec of tables.getRecords()) {
      const tableId = rec.tableId as string;
      knownNames.add(tableId);
      const columns = activeDoc.docData.getMetaTable("_grist_Tables_column");
      for (const col of columns.filterRecords({parentId: rec.id})) {
        knownNames.add(col.colId as string);
      }
    }
    // Also add common system columns
    knownNames.add('manualSort');
    // Quote known names that aren't already quoted
    for (const name of knownNames) {
      if (/[A-Z]/.test(name)) {
        // Only quote names with uppercase (which Postgres would lowercase)
        quotedStatement = quotedStatement.replace(
          new RegExp(`(?<!")\\b${name}\\b(?!")`, 'g'), `"${name}"`
        );
      }
    }
  }
  const wrappedStatement = `select * from (${quotedStatement})`;
  const interrupt = setTimeout(async () => {
    try {
      await activeDoc.docStorage.interrupt();
    } catch (e) {
      // Should be unreachable, but just in case...
      log.error("runSQL interrupt failed with error ", e);
    }
  }, timeout);
  try {
    let rows = await activeDoc.docStorage.all(
      wrappedStatement,
      ...(options.args || []),
    );
    // For Postgres, clean up the result:
    // - Strip gristAlt_* companion columns (internal to Postgres backend)
    // - Decode bytea columns (Any/Blob types stored as marshal binary)
    if (process.env.GRIST_DOC_BACKEND === 'postgres') {
      const marshal = require('app/common/marshal');
      rows = rows.map(row => {
        const clean: Record<string, any> = {};
        for (const [key, val] of Object.entries(row)) {
          if (key.startsWith('gristAlt_')) { continue; }
          if (Buffer.isBuffer(val)) {
            try { clean[key] = marshal.loads(val); } catch { clean[key] = val; }
          } else {
            clean[key] = val;
          }
        }
        return clean;
      });
    }
    return rows;
  } finally {
    clearTimeout(interrupt);
  }
}
