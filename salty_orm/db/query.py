#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Copyright (c) 2018 Robert Abram - All Rights Reserved.
#
#
# A light python ORM, emulating some of the django ORM functionality.
# Does not support joins between tables
#
# Warning: If you add properties to objects, check the clone() or combine() methods
#          to make sure those properties get set in the cloned objects.


from collections import OrderedDict
import json
import datetime
from enum import Enum
from typing import TypeVar, Union

from dateutil.parser import parse as dateparse

from salty_orm.db.base_provider import BaseDBConnection


REPR_OUTPUT_SIZE = 20


def json_datetime_handler(x):
    """ Format datetime objects for the json engine """
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type")


#
# Testing auto complete type hinting with inherited classes.
#
# https://stackoverflow.com/questions/44836545/type-hint-clone-function-in-python-3-5
#
BaseUtilityModel_T = TypeVar('BaseUtilityModel_T', bound='BaseUtilityModel')


class ModelError(Exception):
    """Base class for exceptions in this module."""

    message = None

    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return self.message


class DoesNotExist(ModelError):
    """ python exception class """
    pass


class MultipleObjectsReturned(ModelError):
    """ python exception class """
    pass


class ModelRequired(ModelError):
    """ python exception class """
    pass


class ModelFieldRequired(ModelError):
    """ python exception class """
    pass


class BaseTableModel(object):
    """
    This Model object is used as a base for data retrieval and manipulation
    """

    id = None  # type: int

    # Fields mostly in every table
    created = None  # type: datetime
    modified = None  # type: datetime

    objects = None  # type: BaseQuerySet
    fields = None  # type: list

    def __init__(self, db_conn: BaseDBConnection, *args, **kwargs):
        """
        If parameter values in args, then the value is expected to be a dictionary from a json response
        from the server.

        If parameter values are in kwargs, then they are named parameters passed when the object is instantiated.
        """

        self.db_conn = db_conn
        self.fields = list()
        self.objects = BaseQuerySet(db_conn, model=self)

        if db_conn is not None and not db_conn.testing:
            self.fields = self._get_table_columns()

        if args is not None and len(args) is not 0 and args[0] is not None:
            # self._json_data = True
            for key, value in args[0].items():
                self._set_field_value(key, value)

        else:
            for key, value in kwargs.items():
                self._set_field_value(key, value)

    def get_db_conn(self) -> BaseDBConnection:
        return self.db_conn

    def _sanitize(self, value, delimeter=','):
        """ If value is a type of list object then convert to a string """

        if isinstance(value, str) is True:
            return value

        try:
            new_str = delimeter.join(str(x) for x in value)
            return new_str
        except Exception:
            pass

        return value

    def _set_field_value(self, key, value):

        if key not in self.fields:
            self.fields.append(key)

        # check for datetime field
        # TODO: Find a better way to test for datefield
        if key == 'created' or key == 'modified' or key.endswith('_dt') or key.startswith('last_') \
                or key.endswith('_start') or key.endswith('_from') or key.endswith('_from') \
                or key.endswith('_to'):

            try:
                val = dateparse(value)
                value = val
            except Exception:
                pass

        self.__dict__[key] = value

        # print('{0} : {1}'.format(key, value))

    def clone(self: BaseUtilityModel_T) -> BaseUtilityModel_T:

        clone = self.__class__(db_conn=self.db_conn)

        clone.object = self.objects
        clone.fields = self.fields

        for field in self.fields:
            if field in self.__dict__:
                clone.__dict__[field] = self.__dict__[field]

        return clone

    def to_json(self, cleaned: bool=False) -> str:
        """
        Return a json string of the field values
        :param cleaned: Leave out fields with null values
        :return: string
        """
        data = self.to_dict(cleaned)
        return json.dumps(data, default=json_datetime_handler)

    def to_dict(self, cleaned: bool=False) -> dict:
        """
        Return a dictionary of the field values
        :param cleaned: Leave out fields with null values
        :return: dict
        """
        data = OrderedDict()
        for field in self.fields:
            if cleaned is False or self.__dict__[field] is not None:
                data[field] = self.__dict__[field]
        return data

    def _get_table_columns(self) -> list:
        """
        Query the field names from the table and return the column name list
        # TODO: Move this code to each provider to get table field name list
        """

        fields = list()
        sql = None

        if self.db_conn.provider == 'sqlite3':
            sql = 'PRAGMA table_info("{0}")'.format(self.Meta.db_table)
        if self.db_conn.provider == 'mysql':
            sql = 'SHOW COLUMNS FROM {0}'.format(self.Meta.db_table)

        data = self.db_conn.db_exec_stmt(sql)

        if data:
            for row in data:
                if self.db_conn.provider == 'sqlite3':
                    fields.append(row['name'])
                if self.db_conn.provider == 'mysql':
                    if row['Field'] not in fields:
                        fields.append(row['Field'])

        return fields

    def _insert(self, cleaned: bool=False) -> bool:
        """
        Insert a record
        :param cleaned: Leave out fields with null values
        :return True if successfull otherwise False
        # TODO: Move this code to each provider so we can better handle inserting records
        # TODO: for each provider.
        """

        if self.db_conn is None:
            raise ConnectionError('This object has no database connection.')

        ts = datetime.datetime.utcnow()

        self.created = ts
        self.modified = ts

        sql_cols = '`created`, `modified`, '

        if self.db_conn.provider == 'mysql':
            sql_params = '%s, %s, '.format(self.db_conn.placeholder)
        else:
            sql_params = '{0}, {0}, '.format(self.db_conn.placeholder)

        args = OrderedDict()
        if 'created' in self.fields:
            args['created'] = ts
        if 'modified' in self.fields:
            args['modified'] = ts

        for field in self.fields:

            if field in 'id,created,modified':
                continue

            if cleaned is True:
                if field not in self.__dict__ or self.__dict__[field] is None:
                    continue

                try:
                    if len(self.__dict__[field]) == 0:
                        continue
                except Exception:
                    pass

            sql_cols += '`{0}`, '.format(field)
            sql_params += '{0}, '.format(self.db_conn.placeholder)

            try:
                args[field] = self._sanitize(self.__dict__[field])
            except KeyError:
                raise AttributeError("Found '{0}' in table definition, but missing in model?".format(field))

        sql = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(self.Meta.db_table, sql_cols[:-2], sql_params[:-2])

        self.id = self.db_conn.db_exec_commit(sql, args)

        return self.id is not None

    def _update(self, cleaned: bool=False) -> int:
        """
        Update a record
        :param cleaned: Leave out fields with null values
        :return True if successfull otherwise False
        # TODO: Move this code to each provider so we can better handle updating records
        # TODO: for each provider.
        """

        if self.db_conn is None:
            raise ConnectionError('This object has no database connection.')

        ts = datetime.datetime.utcnow()
        self.modified = ts

        sql = "UPDATE {0} SET `modified` = {1}, ".format(self.Meta.db_table, self.db_conn.placeholder)
        args = OrderedDict()

        if 'modified' in self.fields:
            args['modified'] = ts

        for field in self.fields:

            if field in 'id,created,modified':
                continue

            if cleaned is True:
                if field not in self.__dict__ or self.__dict__[field] is None:
                    continue

                try:
                    if len(self.__dict__[field]) == 0:
                        continue
                except Exception:
                    pass

            sql += '`{0}` = {1}, '.format(field, self.db_conn.placeholder)

            args[field] = self._sanitize(self.__dict__[field])

        sql = sql[:-2]

        sql += ' WHERE `id` = {0}'.format(self.db_conn.placeholder)
        args['id'] = self.id

        return self.db_conn.db_exec_commit(sql, args)

    def save(self, cleaned: bool=False):
        """
        Save current record to database
        :param cleaned: Skip fields with None value or empty lists
        :return: last row id
        """

        if self.db_conn is None:
            raise ConnectionError('This object has no database connection.')

        if self.id is None or self.id == 0:
            self._insert(cleaned)
        else:
            self._update(cleaned)

        return self.objects.get(id=self.id)

    def delete(self):
        """ Delete this table record """

        if self.db_conn is None:
            raise ConnectionError('This object has no database connection.')

        if not self.id:
            raise ValueError('This object does not have a valid id to use')

        sql = 'DELETE FROM {0} WHERE id = {1}'.format(self.Meta.db_table, self.db_conn.placeholder)
        args = OrderedDict()
        args['id'] = self.id

        # Should return a positive integer, either the ID valud or 1 if the delete was successful
        return self.db_conn.db_exec_commit(sql, args)

    def __str__(self):
        result = OrderedDict()

        for field in self.fields:
            try:
                result[field] = self.__dict__[field]
            except KeyError:
                pass

        return str(result)

    # Subclasses db_table to be defined in the Meta class
    class Meta:
        db_table = ''  # type: str


class QOper(Enum):
    """
    Operators for each Where statement clause. IE: id=1.
    https://www.tutorialspoint.com/sqlite/sqlite_operators.htm
    """
    O_BETWEEN = 'BETWEEN'
    O_IN = 'IN'
    O_NOT_IN = 'NOT IN'
    O_LIKE = 'LIKE'
    O_GLOB = 'GLOB'
    O_NOT = 'NOT'
    O_IS_NULL = 'IS NULL'
    O_IS = 'IS'
    O_IS_NOT = 'IS NOT'

    O_EQUAL = '='
    O_DBL_EQUAL = '=='
    O_NOT_EQUAL = '!='
    O_GT_LT = '<>'
    O_GT = '>'
    O_LT = '<'
    O_GT_EQUAL = '>='
    O_LT_EQUAL = '<='
    O_NOT_LT = '!<'
    O_NOT_GT = '!>'


class QConn(Enum):
    """
    Connectors for connecting multiple Where statement clauses together.
    """
    C_AND = 'AND'
    C_OR = 'OR'


class Q(object):
    """
    Emulate a simpler version of the django Q object
    This is really a single linked list of ourselves that outputs the where clause
    """

    _field = None  # type: str
    _field_operator = QOper.O_EQUAL  # type: QOper
    _value = None  # type: Union[str, int, list]
    _between_value = None  # type: Union[str, int]
    placeholder = '??'

    invert = False  # type: bool

    # How we connect one Q object to the next; single linked list.
    child = None  # type: Q
    child_connector = QConn.C_AND  # Type: QConn

    def __init__(self, field: str, operator: QOper=QOper.O_EQUAL, value=None, *args):
        self._field = field
        self._field_operator = operator
        self._value = value

        if operator == QOper.O_BETWEEN:
            if len(args) == 1:
                self._between_value = args[0]
            else:
                raise ValueError('Second value in between operation missing')
        elif operator == QOper.O_IN:
            values = list()
            values.append(value)
            if args:
                for v in args:
                    values.append(v)
            self._value = values

    def add(self, q_object, conn: QConn=QConn.C_AND):
        self.child = q_object
        self.child_connector = conn

    def negate(self):
        self.invert = not self.invert

    def _combine(self, other, conn: QConn=QConn.C_AND):

        if not isinstance(other, Q):
            raise TypeError(other)

        obj = self.__class__(self._field, self._field_operator, self._value)
        obj.invert = self.invert
        obj._between_value = self._between_value

        if self.child:
            obj.child = self.child
            obj.child_connector = self.child_connector
            obj.child.add(other, conn)
        else:
            obj.add(other, conn)

        return obj

    def __or__(self, other):
        return self._combine(other, QConn.C_OR)

    def __and__(self, other):
        return self._combine(other, QConn.C_AND)

    def __invert__(self):
        self.negate()
        return self

    def __str__(self):
        """
        Return the formated where clause
        :return: where clause string
        """

        clause = ''
        invert = ''

        placeholder = self.placeholder

        if isinstance(self._value, datetime.date):
            placeholder = "'{0}'".format(placeholder)

        if self.invert:
            invert = 'NOT '

        if self._field_operator == QOper.O_IS_NULL:
            clause += ' {0} IS {1}NULL'.format(self._field, invert)
        elif self._field_operator == QOper.O_BETWEEN:
            clause += ' {0}{1} {2} {3} AND {3}'.format(
                        invert, self._field, self._field_operator.value, placeholder)
        elif self._field_operator == QOper.O_IN:
            plhs = ', '.join(placeholder for x in self._value)
            clause += ' {0}{1} {2} ({3})'.format(invert, self._field, self._field_operator.value, plhs)
        else:
            clause += ' {0}{1} {2} {3}'.format(invert, self._field, self._field_operator.value, placeholder)

        if self.child:
            clause += ' {0}{1}'.format(self.child_connector.value, str(self.child))

        return clause

    def get_args(self, args=None):
        """
        Return the arguments for the where clause in a list
        :return: list of arguments
        """
        if not args:
            args = list()

        if isinstance(self._value, list):
            for v in self._value:
                args.append(v)
        else:
            args.append(self._value)

        if self._between_value is not None:
            args.append(self._between_value)

        if self.child:
            args = self.child.get_args(args)

        return args


class BaseQuery(object):
    """
    This is the object that holds the options for each SQL statement part, then generates the SQL and
    return the results
    """

    # None or list object with fields used in query or
    _fields = None  # type: list
    _order_by = None  # type: list
    _group_by = None  # type: list

    _where = None  # type: Q
    _distinct = False  # type: bool
    _aggregate = False  # type: list
    _limit = None  # type: int

    _custom_sql = None  # type: str
    _custom_args = None  # type: list

    model = None  # type: BaseUtilityModel_T

    def __init__(self, model: BaseUtilityModel_T = None):

        self.model = model

    def clone(self, **kwargs):
        """
        Creates a copy of the current instance. The 'kwargs' parameter can be
        used by clients to update attributes after copying has taken place.
        """

        clone = self.__class__(model=self.model)

        # Clone our underscore properties
        for k, v in self.__dict__.items():
            if k.startswith('_'):
                clone.__dict__[k] = self.__dict__[k]

        clone.__dict__.update(kwargs)

        return clone

    def set_distinct(self):
        self._distinct = True

    def set_aggregate(self, *args):
        self._aggregate = list()
        for arg in args:
            self._aggregate.append(str(arg))

    def set_fields(self, fields):
        self._fields = fields

    def set_group_by(self, fields):
        self._group_by = fields

    def set_order_by(self, fields):
        self._order_by = fields

    def set_limit(self, limit):
        if not isinstance(limit, int) or limit < 0:
            raise ValueError('Invalid value for LIMIT statement')
        self._limit = limit

    def add_q(self, negate, *args, **kwargs):
        """
        Add Q objects to self._where
        :param negate: Invert the Q object
        :param args: list of Q objects
        :param kwargs: Key/Value dictionary
        """

        if not args and not kwargs:
            raise ValueError('No filter arguments provided')

        q_list = list()

        if kwargs:

            for key in kwargs:
                q_list.append(Q(key, QOper.O_EQUAL, kwargs[key]))

        else:
            for q_object in list(args):
                q_list.append(q_object)

        for q_object in q_list:

            if negate:
                q_object = ~q_object

            if not self._where:
                self._where = q_object
            else:
                self._where.add(q_object)

    def to_sql(self) -> (str, list):
        """
        Generate the SQL statment and return it
        :return: Tuple containing SQL statement and arguments.
        """
        sql, args = self._get_sql_query()
        return sql, args

    def _get_sql_query(self):
        """
        Generate a parameterized sql statment and args list
        # TODO: Move this to the Providers
        :return: SQL statment, args list
        """

        try:
            db_table = self.model.Meta.db_table
            if not db_table:
                raise ModelError('db_table not defined in Model Meta class')
        except Exception:
            raise ModelError('db_table not defined in Model Meta class')

        distinct = '' if self._distinct is False else ' DISTINCT'

        # Setup SELECT fields
        fields = ''
        if self._fields and len(self._fields) > 0:
            fields += ', '.join(self._fields)

        if not fields and not self._aggregate:
            fields = '*'

        if self._aggregate:
            if fields:
                fields += ', ' + ', '.join(self._aggregate)
            else:
                fields = ', '.join(self._aggregate)

        # Setup SELECT WHERE clause
        where = ''
        args = None
        if self._where:
            where = ' WHERE{0}'.format(str(self._where))
            args = self._where.get_args()

        # Setup SELECT GROUP BY clause
        if not self._group_by or len(self._group_by) == 0:
            group_by = ''
        else:
            group_by = ' GROUP BY {0}'.format(', '.join(self._group_by))

        # Setup SELECT ORDER BY clause
        if not self._order_by or len(self._order_by) == 0:
            order_by = ''
        else:
            order_by = ' ORDER BY {0}'.format(', '.join(self._order_by))

        if self._limit is None:
            limit = ''
        else:
            limit = ' LIMIT {0}'.format(self._limit)

        # Build SQL query here
        sql = 'SELECT{0} {1} FROM {2}{3}{4}{5}{6}'.format(distinct, fields, db_table, where, group_by, order_by, limit)

        sql = sql.replace(Q.placeholder,  self.model.get_db_conn().placeholder)

        # TODO: Future: Figure out the best location to set the correct argument value placeholder.
        # TODO: Future: Right now we are defaulting to '?' for the argument value placeholder.

        return sql, args

    def run_query(self, db_conn: BaseDBConnection) -> list:
        """
        Make the database query now
        :return: list of ModelBase objects populated
        :rtype: list[BaseUtilityModel_T]
        """
        if not db_conn:
            raise TypeError('db_conn parameter must be active BaseDBConnection object')

        if not db_conn.db_connected():
            raise ConnectionError('BaseDBConnection object is not connected to a database')

        if not self._custom_sql:
            sql, args = self._get_sql_query()
        else:
            sql = self._custom_sql
            args = self._custom_args

        if args:
            records = db_conn.db_exec_stmt(sql, args)
        else:
            records = db_conn.db_exec_stmt(sql)

        results = list()

        if records:

            # TODO: Future: maybe change this to a fetchmany() and return a smaller set each time like django does

            for record in records:
                model = self.model.__class__(db_conn, record)
                results.append(model)

        return results

    def count(self, db_conn) -> int:
        """
        Return the number of records in the table
        # TODO: Move this to the Providers
        :return: record count
        """

        if not self._custom_sql:
            sql = "SELECT count(1) AS count from {0}".format(self.model.Meta.db_table)
            args = None
        else:
            sql = "SELECT count(1) as count from ({0})".format(self._custom_sql)
            args = self._custom_args

        if args:
            record = db_conn.db_exec_stmt(sql, args)
        else:
            record = db_conn.db_exec_stmt(sql)

        if record:
            return int(record[0]['count'] if isinstance(record, list) else record['count'])

        return 0


class BaseQuerySet(object):
    """
    This is the interface for accessing the underlying database
    """

    _filter = None  # type: str
    _result_cache = None  # type: list
    _db_conn = None  # type: BaseDBConnection

    _group_by = None  # type: list
    _order_by = None  # type: list

    query = None  # type: BaseQuery
    model = None  # type: BaseUtilityModel_T

    def __init__(self, db_conn: BaseDBConnection, model: BaseUtilityModel_T=None, query: BaseQuery=None):

        if not isinstance(model, BaseTableModel):
            raise ModelRequired('model parameter must be a ModelBase object')

        self._db_conn = db_conn
        self.model = model
        self.query = query or BaseQuery(self.model)

    def get_db_conn(self) -> BaseDBConnection:
        """
        Return the databsae connection object.
        :return: Database connection object.
        """
        return self._db_conn

    def _clone(self, **kwargs) -> "BaseQuerySet":
        """
        Creates a copy of the current instance. The 'kwargs' parameter can be
        used by clients to update attributes after copying has taken place.
        """

        query = self.query.clone()
        clone = self.__class__(db_conn=self._db_conn, model=self.model, query=query)

        # Clone our underscore properties
        for k, v in self.__dict__.items():
            if k.startswith('_'):
                clone.__dict__[k] = self.__dict__[k]

        clone.query = query

        clone.__dict__.update(kwargs)

        return clone

    # def __deepcopy__(self, memo):
    #     """
    #     Deep copy of a QuerySet doesn't populate the cache
    #     """
    #     obj = self.__class__(self._db_conn, self.model, self.query)
    #     for k, v in self.__dict__.items():
    #         if k == '_result_cache':
    #             obj.__dict__[k] = None
    #         else:
    #             obj.__dict__[k] = copy.deepcopy(v, memo)
    #     return obj

    def __getstate__(self):
        # Force the cache to be fully populated.
        self._fetch_all()
        obj_dict = self.__dict__.copy()
        return obj_dict

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __repr__(self):
        return super(BaseQuerySet, self).__repr__()
        pass
    #     data = list(self[:REPR_OUTPUT_SIZE + 1])
    #     if len(data) > REPR_OUTPUT_SIZE:
    #         data[-1] = "...(remaining elements truncated)..."
    #     return '<%s %r>' % (self.__class__.__name__, data)

    def __len__(self):
        self._fetch_all()
        return len(self._result_cache)

    def __iter__(self):
        """
        Populate the cache now
        """
        self._fetch_all()
        return iter(self._result_cache)

    def __bool__(self):
        self._fetch_all()
        return bool(self._result_cache)

    def __nonzero__(self):  # Python 2 compatibility
        return type(self).__bool__(self)

    def __getitem__(self, k) -> BaseUtilityModel_T:
        """
        Retrieves a value from the data set
        """
        self._fetch_all()

        if isinstance(k, int):
            if k < 0:
                k += len(self._result_cache)
            if 0 <= k < len(self._result_cache):
                return self._result_cache[k].clone()
            raise IndexError('The index ({0}) is out of range.'.format(k))
        elif isinstance(k, slice):
            start = k.start if k.start else 0
            if start < 0:
                start += len(self._result_cache)
            if start > len(self._result_cache):
                start = len(self._result_cache)
            stop = k.stop if k.stop else len(self._result_cache)
            if stop < 0:
                stop += len(self._result_cache)
            if stop > len(self._result_cache):
                stop = len(self._result_cache)
            return [self._result_cache[x].clone() for x in range(start, stop, k.step if k.step else 1)]
        raise TypeError("Invalid argument type.")

    def raw_query(self, sql, *args, **kwargs) -> "BaseQuerySet":
        """ Allow a user defined SQL statement and arguments """

        if kwargs:
            raise TypeError('Unexpected keyword arguments to raw_query: %s' % (list(kwargs),))

        clone = self._clone()
        clone.query._custom_sql = sql
        clone.query._custom_args = args

        return clone

    def to_sql(self) -> (str, list):
        """
        Return the completed SQL statement. Use this for debugging statements
        :return: Tuple containing SQL statement and argument list.
        """
        return self.query.to_sql()

    def _fetch_all(self):

        if self._result_cache is None:
            result = self.query.run_query(self._db_conn)
            self._result_cache = result

    def values_list(self, *fields, **kwargs) -> "BaseQuerySet":

        if kwargs:
            raise TypeError('Unexpected keyword arguments to values_list: %s' % (list(kwargs),))

        clone = self._clone()
        clone._fields = list(fields)
        clone.query.set_fields(fields)

        return clone

    def order_by(self, *fields, **kwargs) -> "BaseQuerySet":

        if kwargs:
            raise TypeError('Unexpected keyword arguments to order_by: %s' % (list(kwargs),))

        clone = self._clone()
        clone._order_by = list(fields)
        clone.query.set_order_by(fields)

        return clone

    def group_by(self, *fields, **kwargs) -> "BaseQuerySet":

        if kwargs:
            raise TypeError('Unexpected keyword arguments to group_by: %s' % (list(kwargs),))

        clone = self._clone()
        clone._group_by = list(fields)
        clone.query.set_group_by(fields)

        return clone

    def all(self):
        """
        Returns a new QuerySet that is a copy of the current one. This allows a
        QuerySet to proxy for a model manager in some cases.
        """
        return self._clone()

    def limit(self, limit) -> "BaseQuerySet":
        """
        Limit the number of records returned
        """
        clone = self._clone()
        clone.query.set_limit(limit)
        return clone

    def distinct(self, *args, **kwargs) -> "BaseQuerySet":
        """
        Return a distinct set of results
        """
        if args or kwargs:
            raise TypeError('Unexpected keyword arguments to distinct: %s' % (list(kwargs),))

        clone = self._clone()
        clone.query.set_distinct()

        return clone

    def aggregate(self, *args, **kwargs) -> "BaseQuerySet":
        """
        Return an aggregate value from a set of records
        :param args: list of aggregate functions
        """
        if kwargs:
            raise TypeError('Unexpected keyword arguments to distinct: %s' % (list(kwargs),))

        clone = self._clone()
        clone.query.set_aggregate(*args)

        return clone

    def filter(self, *args, **kwargs) -> "BaseQuerySet":
        """
        Returns a new QuerySet instance with the args ANDed to the existing
        set.
        """
        return self._filter_or_exclude(False, *args, **kwargs)

    def exclude(self, *args, **kwargs) -> "BaseQuerySet":
        """
        Returns a new QuerySet instance with NOT (args) ANDed to the existing
        set.
        """
        return self._filter_or_exclude(True, *args, **kwargs)

    def _filter_or_exclude(self, negate, *args, **kwargs) -> "BaseQuerySet":

        clone = self._clone()
        clone.query.add_q(negate, *args, **kwargs)

        return clone

    def get(self, *args, **kwargs) -> BaseUtilityModel_T:
        """
        Performs the query and returns a single object matching the given keyword arguments.

        Parameters can be either Q objects using And/Or IE: get(Q() & Q()) or
        key/value pairs IE: get(id=1, name='zappa')
        """

        clone = self._clone()

        # Check for field/value pairs
        if kwargs:
            for field in kwargs:
                # For the field/value to be in kwargs, the equal sign was used.
                clone = self.filter(Q(field, QOper.O_EQUAL, kwargs[field]))
        elif args:
            clone = self.filter(*args, **kwargs)

        num = len(clone)  # Force _result_cache to be populated
        if num == 1:
            return clone._result_cache[0]
        if not num:
            raise DoesNotExist(
                "%s: query returned no records." %
                self.model.__class__.__name__
            )
        raise MultipleObjectsReturned(
            "get() returned more than one %s -- it returned %s!" %
            (self.model.__class__.__name__, num)
        )

    def count(self) -> int:
        """
        Return the number of records in the table
        :return: record count
        """
        return self.query.count(self._db_conn)



