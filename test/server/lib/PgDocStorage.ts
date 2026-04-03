import {ActiveDoc} from 'app/server/lib/ActiveDoc';
import {makeExceptionalDocSession} from 'app/server/lib/DocSession';
import {TestServer} from 'test/gen-server/apiUtils';
import {createDocTools} from 'test/server/docTools';
import axios from 'axios';
import {assert} from 'chai';
import * as _ from 'lodash';

describe('PgDocStorage', function() {
  this.timeout(30000);

  if (process.env.GRIST_DOC_BACKEND !== 'postgres') {
    return;
  }

  const docTools = createDocTools();
  const fakeSession = makeExceptionalDocSession('system');

  async function fetchData(doc: ActiveDoc, tableId: string) {
    const {tableData} = await doc.fetchTable(fakeSession, tableId, true);
    return _.omit(tableData[3], 'manualSort');
  }

  function getSchema(doc: ActiveDoc): string {
    return (doc.docStorage as any)._schema ||
      'doc_' + doc.docName.replace(/[^a-zA-Z0-9_]/g, '_').slice(0, 60).toLowerCase();
  }

  function getPgPool(): any {
    const {Pool} = require('pg');
    return new Pool({connectionString: process.env.GRIST_DOC_POSTGRES_URL});
  }

  it('should create a document and add data', async function() {
    const doc = await docTools.createDoc('PgBasic');
    await doc.applyUserActions(fakeSession, [
      ['AddTable', 'People', [{id: 'Name', type: 'Text'}, {id: 'Age', type: 'Int'}]],
      ['AddRecord', 'People', null, {Name: 'Alice', Age: 30}],
    ]);
    const data = await fetchData(doc, 'People');
    assert.deepEqual(data.Name, ['Alice']);
    assert.deepEqual(data.Age, [30]);
  });

  it('should handle multiple records and updates', async function() {
    const doc = await docTools.createDoc('PgBulk');
    await doc.applyUserActions(fakeSession, [
      ['AddTable', 'Items', [{id: 'Label', type: 'Text'}, {id: 'Count', type: 'Int'}]],
      ['BulkAddRecord', 'Items', [null, null, null], {
        Label: ['A', 'B', 'C'],
        Count: [10, 20, 30],
      }],
    ]);
    let data = await fetchData(doc, 'Items');
    assert.deepEqual(data.Label, ['A', 'B', 'C']);
    assert.deepEqual(data.Count, [10, 20, 30]);

    // Update
    await doc.applyUserActions(fakeSession, [
      ['UpdateRecord', 'Items', 2, {Label: 'B2', Count: 25}],
    ]);
    data = await fetchData(doc, 'Items');
    assert.deepEqual(data.Label, ['A', 'B2', 'C']);
    assert.deepEqual(data.Count, [10, 25, 30]);

    // Delete
    await doc.applyUserActions(fakeSession, [
      ['RemoveRecord', 'Items', 1],
    ]);
    data = await fetchData(doc, 'Items');
    assert.deepEqual(data.Label, ['B2', 'C']);
  });

  it('should handle all basic column types', async function() {
    const doc = await docTools.createDoc('PgTypes');
    await doc.applyUserActions(fakeSession, [
      ['AddTable', 'TypeTest', [
        {id: 'T', type: 'Text'},
        {id: 'I', type: 'Int'},
        {id: 'N', type: 'Numeric'},
        {id: 'B', type: 'Bool'},
        {id: 'D', type: 'Date'},
        {id: 'DT', type: 'DateTime'},
      ]],
      ['AddRecord', 'TypeTest', null, {
        T: 'hello',
        I: 42,
        N: 3.14,
        B: true,
        D: 1710460800,     // 2024-03-15 as epoch seconds
        DT: 1710460800,
      }],
      ['AddRecord', 'TypeTest', null, {
        T: '',
        I: 0,
        N: 0,
        B: false,
        D: null,
        DT: null,
      }],
    ]);
    const data = await fetchData(doc, 'TypeTest');
    assert.deepEqual(data.T, ['hello', '']);
    assert.deepEqual(data.I, [42, 0]);
    assert.deepEqual(data.N, [3.14, 0]);
    assert.deepEqual(data.B, [true, false]);
    assert.deepEqual(data.D, [1710460800, null]);
    assert.deepEqual(data.DT, [1710460800, null]);
  });

  it('should handle schema changes', async function() {
    const doc = await docTools.createDoc('PgSchema');
    await doc.applyUserActions(fakeSession, [
      ['AddTable', 'T1', [{id: 'A', type: 'Text'}]],
      ['AddRecord', 'T1', null, {A: 'x'}],
    ]);
    // Add column
    await doc.applyUserActions(fakeSession, [
      ['AddColumn', 'T1', 'B', {type: 'Int'}],
    ]);
    await doc.applyUserActions(fakeSession, [
      ['UpdateRecord', 'T1', 1, {B: 99}],
    ]);
    let data = await fetchData(doc, 'T1');
    assert.deepEqual(data.A, ['x']);
    assert.deepEqual(data.B, [99]);

    // Rename column
    await doc.applyUserActions(fakeSession, [
      ['RenameColumn', 'T1', 'B', 'Score'],
    ]);
    data = await fetchData(doc, 'T1');
    assert.deepEqual(data.Score, [99]);

    // Remove column
    await doc.applyUserActions(fakeSession, [
      ['RemoveColumn', 'T1', 'Score'],
    ]);
    data = await fetchData(doc, 'T1');
    assert.isUndefined(data.Score);
    assert.deepEqual(data.A, ['x']);
  });

  it('should handle formulas', async function() {
    const doc = await docTools.createDoc('PgFormula');
    await doc.applyUserActions(fakeSession, [
      ['AddTable', 'Calc', [
        {id: 'X', type: 'Numeric'},
        {id: 'Y', type: 'Numeric'},
        {id: 'Sum', type: 'Numeric', isFormula: true, formula: '$X + $Y'},
      ]],
      ['AddRecord', 'Calc', null, {X: 10, Y: 20}],
      ['AddRecord', 'Calc', null, {X: 3, Y: 7}],
    ]);
    const data = await fetchData(doc, 'Calc');
    assert.deepEqual(data.X, [10, 3]);
    assert.deepEqual(data.Y, [20, 7]);
    assert.deepEqual(data.Sum, [30, 10]);
  });

  it('should store native Postgres types queryable by external tools', async function() {
    const doc = await docTools.createDoc('PgNative');
    await doc.applyUserActions(fakeSession, [
      ['AddTable', 'Typed', [
        {id: 'Name', type: 'Text'},
        {id: 'Active', type: 'Bool'},
        {id: 'HireDate', type: 'Date'},
        {id: 'LastSeen', type: 'DateTime'},
        {id: 'Score', type: 'Numeric'},
      ]],
      ['AddRecord', 'Typed', null, {
        Name: 'Alice', Active: true, HireDate: 1710460800,
        LastSeen: 1710460800, Score: 95.5,
      }],
    ]);

    // Verify Grist reads it back correctly
    const data = await fetchData(doc, 'Typed');
    assert.deepEqual(data.Name, ['Alice']);
    assert.deepEqual(data.Active, [true]);
    assert.deepEqual(data.HireDate, [1710460800]);
    assert.deepEqual(data.LastSeen, [1710460800]);
    assert.deepEqual(data.Score, [95.5]);

    // Verify native types via direct Postgres query
    const pool = getPgPool();
    const schema = getSchema(doc);

    const typeInfo = await pool.query(
      `SELECT column_name, data_type FROM information_schema.columns
       WHERE table_schema = $1 AND table_name = 'Typed'
       AND column_name NOT LIKE 'gristAlt_%' AND column_name != 'manualSort'
       ORDER BY ordinal_position`, [schema]
    );
    const types: Record<string, string> = {};
    for (const row of typeInfo.rows) { types[row.column_name] = row.data_type; }

    assert.equal(types.id, 'integer');
    assert.equal(types.Name, 'text');
    assert.equal(types.Active, 'boolean');
    assert.equal(types.HireDate, 'date');
    assert.equal(types.LastSeen, 'timestamp with time zone');
    assert.equal(types.Score, 'numeric');

    // Verify an external tool can query with native types
    await pool.query(`SET search_path TO "${schema}"`);
    const directResult = await pool.query(
      `SELECT "Name", "Active", "HireDate", "Score" FROM "Typed"`
    );
    const row = directResult.rows[0];
    assert.equal(row.Name, 'Alice');
    assert.equal(row.Active, true);
    assert.equal(row.Score, 95.5);
    // HireDate is a JS Date from node-postgres
    assert.instanceOf(row.HireDate, Date);

    await pool.end();
  });

  it('should preserve non-conforming values in alt column', async function() {
    const doc = await docTools.createDoc('PgAlt');
    await doc.applyUserActions(fakeSession, [
      ['AddTable', 'Mixed', [{id: 'Val', type: 'Numeric'}]],
      ['AddRecord', 'Mixed', null, {Val: 42}],          // conforming
      ['AddRecord', 'Mixed', null, {Val: 'banana'}],     // non-conforming string
      ['AddRecord', 'Mixed', null, {Val: ['E', 'TypeError'] as any}], // error value
    ]);

    // Grist should see all values preserved
    const data = await fetchData(doc, 'Mixed');
    assert.deepEqual(data.Val, [42, 'banana', ['E', 'TypeError']] as any);

    // Native column should have 42 for row 1, NULL for rows 2-3
    const pool = getPgPool();
    await pool.query(`SET search_path TO "${getSchema(doc)}"`);
    const r = await pool.query(
      `SELECT "Val", "gristAlt_Val" FROM "Mixed" ORDER BY id`
    );
    assert.equal(r.rows[0].Val, 42);
    assert.isNull(r.rows[0].gristAlt_Val);  // conforming: alt is NULL
    assert.isNull(r.rows[1].Val);            // non-conforming: native is NULL
    assert.equal(r.rows[1].gristAlt_Val, 'banana'); // alt has the raw value (jsonb parsed by node-pg)
    assert.isNull(r.rows[2].Val);
    assert.deepEqual(r.rows[2].gristAlt_Val, ['E', 'TypeError']);

    await pool.end();
  });

  it('should handle ChoiceList and RefList as native arrays', async function() {
    const doc = await docTools.createDoc('PgArrays');
    await doc.applyUserActions(fakeSession, [
      ['AddTable', 'Tags', [{id: 'Label', type: 'Text'}]],
      ['BulkAddRecord', 'Tags', [null, null], {Label: ['Red', 'Blue']}],
      ['AddTable', 'Items', [
        {id: 'Colors', type: 'ChoiceList'},
        {id: 'TagRefs', type: 'RefList:Tags'},
      ]],
      ['AddRecord', 'Items', null, {Colors: ['L', 'Red', 'Blue'] as any, TagRefs: ['L', 1, 2] as any}],
    ]);

    // Grist round-trip
    const data = await fetchData(doc, 'Items');
    assert.deepEqual(data.Colors, [['L', 'Red', 'Blue']] as any);
    assert.deepEqual(data.TagRefs, [['L', 1, 2]] as any);

    // Verify native Postgres arrays via direct SQL
    const pool = getPgPool();
    await pool.query(`SET search_path TO "${getSchema(doc)}"`);
    const r = await pool.query('SELECT "Colors", "TagRefs" FROM "Items"');
    assert.deepEqual(r.rows[0].Colors, ['Red', 'Blue']);
    assert.deepEqual(r.rows[0].TagRefs, [1, 2]);
    await pool.end();
  });

  it('should reopen a document after shutdown', async function() {
    const doc1 = await docTools.createDoc('PgReopen');
    const actualName = doc1.docName;
    await doc1.applyUserActions(fakeSession, [
      ['AddTable', 'Persist', [{id: 'Val', type: 'Text'}]],
      ['AddRecord', 'Persist', null, {Val: 'before shutdown'}],
    ]);
    await doc1.shutdown();
    const doc2 = await docTools.loadDoc(actualName);
    const data = await fetchData(doc2, 'Persist');
    assert.deepEqual(data.Val, ['before shutdown']);
  });

  it('should handle multiple documents coexisting', async function() {
    const docA = await docTools.createDoc('PgMultiA');
    const docB = await docTools.createDoc('PgMultiB');
    await docA.applyUserActions(fakeSession, [
      ['AddTable', 'T', [{id: 'X', type: 'Int'}]],
      ['AddRecord', 'T', null, {X: 1}],
    ]);
    await docB.applyUserActions(fakeSession, [
      ['AddTable', 'T', [{id: 'X', type: 'Int'}]],
      ['AddRecord', 'T', null, {X: 2}],
    ]);
    const dataA = await fetchData(docA, 'T');
    const dataB = await fetchData(docB, 'T');
    assert.deepEqual(dataA.X, [1]);
    assert.deepEqual(dataB.X, [2]);
  });

  it('should handle formula errors in alt column', async function() {
    const doc = await docTools.createDoc('PgFormulaErr');
    await doc.applyUserActions(fakeSession, [
      ['AddTable', 'Err', [
        {id: 'X', type: 'Numeric'},
        {id: 'Inv', type: 'Numeric', isFormula: true, formula: '1 / $X'},
      ]],
      ['AddRecord', 'Err', null, {X: 2}],    // Inv = 0.5
      ['AddRecord', 'Err', null, {X: 0}],    // Inv = ZeroDivisionError
    ]);
    const data = await fetchData(doc, 'Err');
    assert.equal(data.Inv[0], 0.5);
    // The second value should be an error
    assert.isArray(data.Inv[1]);
    assert.equal((data.Inv[1] as any)[0], 'E');
  });
});

describe('PgDocStorageManager', function() {
  this.timeout(30000);

  if (process.env.GRIST_DOC_BACKEND !== 'postgres') {
    return;
  }

  let pool: any;
  let mgr: any;

  before(async function() {
    const {Pool} = require('pg');
    const {PgDocStorageManager} = require('app/server/lib/PgDocStorageManager');
    pool = new Pool({connectionString: process.env.GRIST_DOC_POSTGRES_URL});
    mgr = new PgDocStorageManager(pool);
  });

  after(async function() {
    await pool.end();
  });

  it('should detect new vs existing documents', async function() {
    await pool.query('DROP SCHEMA IF EXISTS "doc_mgr_test" CASCADE');
    assert.isTrue(await mgr.prepareLocalDoc('mgr_test'));  // new
    await pool.query('CREATE SCHEMA "doc_mgr_test"');
    assert.isFalse(await mgr.prepareLocalDoc('mgr_test'));  // exists
    await pool.query('DROP SCHEMA "doc_mgr_test"');
  });

  it('should delete a document schema', async function() {
    await pool.query('CREATE SCHEMA IF NOT EXISTS "doc_mgr_del"');
    await pool.query('CREATE TABLE "doc_mgr_del".test (id int)');
    await mgr.deleteDoc('mgr_del');
    const r = await pool.query(
      `SELECT 1 FROM information_schema.schemata WHERE schema_name = 'doc_mgr_del'`
    );
    assert.equal(r.rows.length, 0);
  });

  it('should rename a document schema', async function() {
    await pool.query('DROP SCHEMA IF EXISTS "doc_mgr_old" CASCADE');
    await pool.query('DROP SCHEMA IF EXISTS "doc_mgr_new" CASCADE');
    await pool.query('CREATE SCHEMA "doc_mgr_old"');
    await mgr.renameDoc('mgr_old', 'mgr_new');
    const r = await pool.query(
      `SELECT 1 FROM information_schema.schemata WHERE schema_name = 'doc_mgr_new'`
    );
    assert.equal(r.rows.length, 1);
    await pool.query('DROP SCHEMA "doc_mgr_new"');
  });

  it('should list document schemas', async function() {
    await pool.query('CREATE SCHEMA IF NOT EXISTS "doc_mgr_list1"');
    await pool.query('CREATE SCHEMA IF NOT EXISTS "doc_mgr_list2"');
    const docs = await mgr.listDocs();
    const names = docs.map((d: any) => d.name);
    assert.include(names, 'doc_mgr_list1');
    assert.include(names, 'doc_mgr_list2');
    await pool.query('DROP SCHEMA "doc_mgr_list1"');
    await pool.query('DROP SCHEMA "doc_mgr_list2"');
  });

  it('should fork a document', async function() {
    await pool.query('DROP SCHEMA IF EXISTS "doc_mgr_src" CASCADE');
    await pool.query('DROP SCHEMA IF EXISTS "doc_mgr_dst" CASCADE');
    await pool.query('CREATE SCHEMA "doc_mgr_src"');
    await pool.query('CREATE TABLE "doc_mgr_src".data (id int, val text)');
    await pool.query('INSERT INTO "doc_mgr_src".data VALUES (1, \'hello\')');
    await mgr.prepareFork('mgr_src', 'mgr_dst');
    await pool.query('SET search_path TO "doc_mgr_dst"');
    const r = await pool.query('SELECT val FROM data WHERE id = 1');
    assert.equal(r.rows[0].val, 'hello');
    await pool.query('DROP SCHEMA "doc_mgr_src" CASCADE');
    await pool.query('DROP SCHEMA "doc_mgr_dst" CASCADE');
  });
});

describe('PgDocStorage via HTTP API', function() {
  this.timeout(30000);

  if (process.env.GRIST_DOC_BACKEND !== 'postgres') {
    return;
  }

  let server: TestServer;
  let serverUrl: string;

  before(async function() {
    server = new TestServer(this);
    serverUrl = await server.start(['home', 'docs'], {}, {seedData: true});
  });

  after(async function() {
    await server.stop();
  });

  it('should create a document with Table1 via API', async function() {
    const cookie = await server.getCookieLogin('nasa', {
      email: 'chimpy@getgrist.com',
      name: 'Chimpy',
    });

    // Create a new document (this uses GRIST_DOC_WITH_TABLE1_SQL)
    const resp = await axios.post(`${serverUrl}/api/docs`, {name: 'PgApiTest'}, cookie);
    assert.equal(resp.status, 200);
    const docId = resp.data;

    // Table1 should exist — add a record to it
    const addResp = await axios.post(
      `${serverUrl}/api/docs/${docId}/tables/Table1/records`,
      {records: [{fields: {A: 'via API', B: 99, C: true}}]},
      cookie
    );
    assert.equal(addResp.status, 200);

    // Fetch it back
    const fetchResp = await axios.get(
      `${serverUrl}/api/docs/${docId}/tables/Table1/records`,
      cookie
    );
    assert.equal(fetchResp.status, 200);
    const records = fetchResp.data.records;
    assert.isAtLeast(records.length, 1);
    assert.equal(records[0].fields.A, 'via API');
    assert.equal(records[0].fields.B, 99);
    assert.equal(records[0].fields.C, true);
  });
});
