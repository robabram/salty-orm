#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Copyright (c) 2018 Robert Abram - All Rights Reserved.
#
from salty_orm.db.query import ModelFieldRequired


class _BaseAggregateFunction(object):
    """ A base class for aggregate functions """
    _field = None
    _alias = None
    _func = 'Unknown'

    def __init__(self, column: str, alias: str = None):
        """
        Set an aggregate function
        :param column: Database field column name.
        :param alias: Change default alias to a specific alias name.
        """
        if not column or (not isinstance(column, str) and not isinstance(column, int)):
            raise ModelFieldRequired('A field name or expression is required.')
        self._field = column
        self._alias = alias

    def __repr__(self):
        if not self._alias:
            self._alias = '{0}__{1}'.format(self._field, self._func.lower())
        return '{0} ({1}) as {2}'.format(self._func, self._field, self._alias)

    def __str__(self):
        return self.__repr__()


class Max(_BaseAggregateFunction):
    """
    Return the maximum value from a field in a query.
    """
    _func = 'MAX'

    def __init__(self,  column: str, alias: str = None):
        """
        Aggregate the maximum column field value.
        :param column: Database field column name.
        :param alias: Change default alias to a specific alias name.
        """
        super(Max, self).__init__(column, alias)


class Min(_BaseAggregateFunction):
    """
    Return the minimum value from a field in query.
    """
    _func = 'MIN'

    def __init__(self,  column: str, alias: str = None):
        """
        Aggregate the minimum column field value.
        :param column: Database field column name.
        :param alias: Change default alias to a specific alias name.
        """
        super(Min, self).__init__(column, alias)


class Count(_BaseAggregateFunction):
    """
    Return the count of value from a field column.
    """
    _func = 'COUNT'

    def __init__(self,  column: str, alias: str = None):
        """
        Count the column value.
        :param column: Database field column name.
        :param alias: Change default alias to a specific alias name.
        """
        super(Count, self).__init__(column, alias)


class Sum(_BaseAggregateFunction):
    """
    Return the sum of values from a column field in a query.
    """
    _func = 'SUM'

    def __init__(self,  column: str, alias: str = None):
        """
        Sum the column value.
        :param column: Database field column name.
        :param alias: Change default alias to a specific alias name.
        """
        super(Sum, self).__init__(column, alias)
