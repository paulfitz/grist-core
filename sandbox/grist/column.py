import types
from collections import namedtuple

import depend
import objtypes
import usertypes
import relabeling
import relation
import moment
import logger
from sortedcontainers import SortedListWithKey

log = logger.Logger(__name__, logger.INFO)

MANUAL_SORT = 'manualSort'
MANUAL_SORT_COL_INFO = {
  'id': MANUAL_SORT,
  'type': 'ManualSortPos',
  'formula': '',
  'isFormula': False
}
MANUAL_SORT_DEFAULT = 2147483647.0

SPECIAL_COL_IDS = {'id', MANUAL_SORT}

def is_user_column(col_id):
  """
  Returns whether the col_id is of a user column (as opposed to special columns that can't be used
  for user data).
  """
  return col_id not in SPECIAL_COL_IDS and not col_id.startswith('#')

def is_visible_column(col_id):
  """
  Returns whether this is an id of a column that's intended to be shown to the user.
  """
  return is_user_column(col_id) and not col_id.startswith('gristHelper_')

def is_virtual_column(col_id):
  """
  Returns whether col_id is of a special column that does not get communicated outside of the
  sandbox. Lookup maps are an example.
  """
  return col_id.startswith('#')

def is_validation_column_name(name):
  return name.startswith("validation___")

ColInfo = namedtuple('ColInfo', ('type_obj', 'is_formula', 'method'))

def get_col_info(col_model, default_func=None):
  if isinstance(col_model, types.FunctionType):
    type_obj = getattr(col_model, 'grist_type', usertypes.Any())
    return ColInfo(type_obj, is_formula=True, method=col_model)
  else:
    return ColInfo(col_model, is_formula=False, method=col_model.default_func or default_func)


class BaseColumn(object):
  """
  BaseColumn holds a column of data, whether raw or computed.
  """
  def __init__(self, table, col_id, col_info):
    self.type_obj = col_info.type_obj
    self._data = []
    self.col_id = col_id
    self.table_id = table.table_id
    self.node = depend.Node(self.table_id, col_id)
    self._is_formula = col_info.is_formula
    self._is_private = bool(col_info.method) and getattr(col_info.method, 'is_private', False)
    self.method = col_info.method

    # Always initialize to include the special empty record at index 0.
    self.growto(1)

  def update_method(self, method):
    """
    After rebuilding user code, we reuse existing column objects, but need to replace their
    'method' function. The method may refer to variables in the generated "usercode" module, and
    it's important that all such references are to the rebuilt "usercode" module.
    """
    self.method = method

  def is_formula(self):
    """
    Whether this is a formula column. Note that a non-formula column may have an associated
    method, which is used to fill in defaults when a record is added.
    """
    return self._is_formula

  def is_private(self):
    """
    Returns whether this method is private to the sandbox. If so, changes to this column do not
    get communicated to outside the sandbox via actions.
    """
    return self._is_private

  def has_formula(self):
    """
    has_formula is true if formula is set, whether or not this is a formula column.
    """
    return self.method is not None

  def clear(self):
    self._data = []
    self.growto(1)    # Always include the special empty record at index 0.

  def destroy(self):
    """
    Called when the column is deleted.
    """
    del self._data[:]

  def growto(self, size):
    if len(self._data) < size:
      self._data.extend([self.getdefault()] * (size - len(self._data)))

  def size(self):
    return len(self._data)

  def set(self, row_id, value):
    """
    Sets the value of this column for the given row_id. Value should be as returned by convert(),
    i.e. of the right type, or alttext, or error (but should NOT be random wrong types).
    """
    try:
      self._data[row_id] = value
    except IndexError:
      self.growto(row_id + 1)
      self._data[row_id] = value

  def unset(self, row_id):
    """
    Sets the value for the given row_id to the default value.
    """
    self.set(row_id, self.getdefault())

  def get_cell_value(self, row_id):
    """
    Returns the "rich" value for the given row_id, i.e. the value that would be seen by formulas.
    E.g. for ReferenceColumns it'll be the referred-to Record object. For cells containing
    alttext, this will be an AltText object. For RaisedException objects that represent a thrown
    error, this will re-raise that error.
    """
    raw = self.raw_get(row_id)
    if isinstance(raw, objtypes.RaisedException):
      raise raw.error
    if self.type_obj.is_right_type(raw):
      return self._make_rich_value(raw)
    return usertypes.AltText(str(raw), self.type_obj.typename())

  def _make_rich_value(self, typed_value):
    """
    Called by get_cell_value() with a value of the right type for this column. Should be
    implemented by derived classes to produce a "rich" version of the value.
    """
    # pylint: disable=no-self-use
    return typed_value

  def raw_get(self, row_id):
    """
    Returns the value stored for the given row_id. This may be an error or alttext, and it does
    not convert to a richer object.
    """
    try:
      return self._data[row_id]
    except IndexError:
      return self.getdefault()

  def safe_get(self, row_id):
    """
    Returns a value of the right type, or the default value if the stored value had a wrong type.
    """
    raw = self.raw_get(row_id)
    return raw if self.type_obj.is_right_type(raw) else self.getdefault()

  def getdefault(self):
    """
    Returns the default value for this column. This is a static default; the implementation of
    "default formula" logic is separate.
    """
    return self.type_obj.default

  def sample_value(self):
    """
    Returns a sample value for this column, used for auto-completions. E.g. for a date, this
    returns an actual datetime object rather than None (only its attributes should matter).
    """
    return self.type_obj.default

  def copy_from_column(self, other_column):
    """
    Replace this column's data entirely with data from another column of the same exact type.
    """
    self._data[:] = other_column._data

  def convert(self, value_to_convert):
    """
    Converts a value of any type to this column's type, returning either the converted value (for
    which is_right_type is true), or an alttext string, or an error object.
    """
    return self.type_obj.convert(value_to_convert)

  def prepare_new_values(self, values, ignore_data=False, action_summary=None):
    """
    This allows us to modify values and also produce adjustments to existing records. This
    currently is only used by PositionColumn. Returns two lists: new_values, and
    [(row_id, new_value)] list of adjustments to existing records.
    If ignore_data is True, makes adjustments without regard to the existing data; this is used
    for processing ReplaceTableData actions.
    """
    # pylint: disable=no-self-use, unused-argument
    return values, []


class DataColumn(BaseColumn):
  """
  DataColumn describes a column of raw data, and holds it.
  """
  pass

class BoolColumn(BaseColumn):
  def set(self, row_id, value):
    # When 1 or 1.0 is loaded, we should see it as True, and similarly 0 as False. This is similar
    # to how, after loading a number into a DateColumn, we should see a date, except we adjust
    # booleans at set() time.
    bool_value = True if value == 1 else (False if value == 0 else value)
    super(BoolColumn, self).set(row_id, bool_value)

class NumericColumn(BaseColumn):
  def set(self, row_id, value):
    # Make sure any integers are treated as floats to avoid truncation.
    # Uses `type(value) == int` rather than `isintance(value, int)` to specifically target
    # ints and not bools (which are singleton instances the class int in python).  But
    # perhaps something should be done about bools also?
    # pylint: disable=unidiomatic-typecheck
    super(NumericColumn, self).set(row_id, float(value) if type(value) == int else value)

_sample_date = moment.ts_to_date(0)
_sample_datetime = moment.ts_to_dt(0, None, moment.TZ_UTC)

class DateColumn(NumericColumn):
  """
  DateColumn contains numerical timestamps represented as seconds since epoch, in type float,
  to midnight of specific UTC dates. Accessing them yields date objects.
  """
  def _make_rich_value(self, typed_value):
    return typed_value and moment.ts_to_date(typed_value)

  def sample_value(self):
    return _sample_date

class DateTimeColumn(NumericColumn):
  """
  DateTimeColumn contains numerical timestamps represented as seconds since epoch, in type float,
  and a timestamp associated with the column. Accessing them yields datetime objects.
  """
  def __init__(self, table, col_id, col_info):
    super(DateTimeColumn, self).__init__(table, col_id, col_info)
    self._timezone = col_info.type_obj.timezone

  def _make_rich_value(self, typed_value):
    return typed_value and moment.ts_to_dt(typed_value, self._timezone)

  def sample_value(self):
    return _sample_datetime

class PositionColumn(NumericColumn):
  def __init__(self, table, col_id, col_info):
    super(PositionColumn, self).__init__(table, col_id, col_info)
    # This is a list of row_ids, ordered by the position.
    self._sorted_rows = SortedListWithKey(key=self.raw_get)

  def set(self, row_id, value):
    self._sorted_rows.discard(row_id)
    super(PositionColumn, self).set(row_id, value)
    if value != self.getdefault():
      self._sorted_rows.add(row_id)

  def copy_from_column(self, other_column):
    super(PositionColumn, self).copy_from_column(other_column)
    self._sorted_rows = SortedListWithKey(other_column._sorted_rows[:], key=self.raw_get)

  def prepare_new_values(self, values, ignore_data=False, action_summary=None):
    # This does the work of adjusting positions and relabeling existing rows with new position
    # (without changing sort order) to make space for the new positions. Note that this is also
    # used for updating a position for an existing row: we'll find a new value for it; later when
    # this value is set, the old position will be removed and the new one added.
    if ignore_data:
      rows = SortedListWithKey([], key=self.raw_get)
    else:
      rows = self._sorted_rows
    adjustments, new_values = relabeling.prepare_inserts(rows, values)
    return new_values, [(self._sorted_rows[i], pos) for (i, pos) in adjustments]


class BaseReferenceColumn(BaseColumn):
  """
  Base class for ReferenceColumn and ReferenceListColumn.
  """
  def __init__(self, table, col_id, col_info):
    super(BaseReferenceColumn, self).__init__(table, col_id, col_info)
    # We can assume that all tables have been instantiated, but not all initialized.
    target_table_id = self.type_obj.table_id
    self._target_table = table._engine.tables.get(target_table_id, None)
    self._relation = relation.ReferenceRelation(table.table_id, target_table_id, col_id)
    # Note that we need to remove these back-references when the column is removed.
    if self._target_table:
      self._target_table._back_references.add(self)

  def destroy(self):
    # Destroy the column and remove the back-reference we created in the constructor.
    super(BaseReferenceColumn, self).destroy()
    if self._target_table:
      self._target_table._back_references.remove(self)

  def _update_references(self, row_id, old_value, new_value):
    raise NotImplementedError()

  def set(self, row_id, value):
    old = self.safe_get(row_id)
    super(BaseReferenceColumn, self).set(row_id, value)
    new = self.safe_get(row_id)
    self._update_references(row_id, old, new)

  def copy_from_column(self, other_column):
    super(BaseReferenceColumn, self).copy_from_column(other_column)
    self._relation.clear()
    # This is hacky: we should have an interface to iterate through values of a column. (As it is,
    # self._data may include values for non-existent rows; it works here because those values are
    # falsy, which makes them ignored by self._update_references).
    for row_id, value in enumerate(self._data):
      if self.type_obj.is_right_type(value):
        self._update_references(row_id, None, value)

  def sample_value(self):
    return self._target_table.sample_record


class ReferenceColumn(BaseReferenceColumn):
  """
  ReferenceColumn contains IDs of rows in another table. Accessing them yields the records in the
  other table.
  """
  def _make_rich_value(self, typed_value):
    # If we refer to an invalid table, return integers rather than fail completely.
    if not self._target_table:
      return typed_value
    # For a Reference, values must either refer to an existing record, or be 0. In all tables,
    # the 0 index will contain the all-defaults record.
    return self._target_table.Record(self._target_table, typed_value, self._relation)

  def _update_references(self, row_id, old_value, new_value):
    if old_value:
      self._relation.remove_reference(row_id, old_value)
    if new_value:
      self._relation.add_reference(row_id, new_value)

  def prepare_new_values(self, values, ignore_data=False, action_summary=None):
    if action_summary and values:
      values = action_summary.translate_new_row_ids(self._target_table.table_id, values)
    return values, []


class ReferenceListColumn(BaseReferenceColumn):
  """
  ReferenceListColumn maintains for each row a list of references (row IDs) into another table.
  Accessing them yields RecordSets.
  """
  def _update_references(self, row_id, old_list, new_list):
    for old_value in old_list or ():
      self._relation.remove_reference(row_id, old_value)
    for new_value in new_list or ():
      self._relation.add_reference(row_id, new_value)

  def _make_rich_value(self, typed_value):
    if typed_value is None:
      typed_value = []
    # If we refer to an invalid table, return integers rather than fail completely.
    if not self._target_table:
      return typed_value
    return self._target_table.RecordSet(self._target_table, typed_value, self._relation)


# Set up the relationship between usertypes objects and column objects.
usertypes.BaseColumnType.ColType = DataColumn
usertypes.Reference.ColType = ReferenceColumn
usertypes.ReferenceList.ColType = ReferenceListColumn
usertypes.DateTime.ColType = DateTimeColumn
usertypes.Date.ColType = DateColumn
usertypes.PositionNumber.ColType = PositionColumn
usertypes.Bool.ColType = BoolColumn
usertypes.Numeric.ColType = NumericColumn

def create_column(table, col_id, col_info):
  return col_info.type_obj.ColType(table, col_id, col_info)
