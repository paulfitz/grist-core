import {ActiveDoc} from 'app/server/lib/ActiveDoc';
import {makeExceptionalDocSession} from 'app/server/lib/DocSession';
import {createDocTools} from 'test/server/docTools';
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
});
