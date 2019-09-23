#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Copyright (c) 2019 Robert Abram - All Rights Reserved.
#
import unittest

from salty_orm.db.model import Max, Min, Count, Sum
from salty_orm.db.mysql_provider import MySQLDBConnection
from salty_orm.examples.models import RulingModel


class TestQueryStructure(unittest.TestCase):

    _provider = None

    def setUp(self) -> None:
        self._provider = MySQLDBConnection(testing=True)
        return super(TestQueryStructure, self).setUp()

    def test_simple_select(self):
        """ test simple select returning all columns and rows """
        sql, args = RulingModel(self._provider).objects.to_sql()
        self.assertEqual(sql, 'SELECT * FROM test_model')

    def test_simple_select_specific_columsn(self):
        """ Test simple select returning a column list and all rows """
        sql, args = RulingModel(self._provider).objects.values_list("id", "name").to_sql()
        self.assertEqual(sql, "SELECT id, name FROM test_model")

    def test_max_column(self):
        """ Test a maximum column select, no group by clause """
        sql, args = RulingModel(self._provider).objects.aggregate(Max('id')).to_sql()
        self.assertEqual(sql, "SELECT *, MAX(id) as id__max FROM test_model")

    def test_min_column(self):
        """ Test a minimum column select, no group by clause """
        sql, args = RulingModel(self._provider).objects.aggregate(Min('id')).to_sql()
        self.assertEqual(sql, "SELECT *, MIN(id) as id__min FROM test_model")

    def test_count_column(self):
        """ Test a count column select, no group by clause """
        sql, args = RulingModel(self._provider).objects.aggregate(Count('id')).to_sql()
        self.assertEqual(sql, "SELECT *, COUNT(id) as id__count FROM test_model")

    def test_sum_column(self):
        """ Test a sum column select, no group by clause """
        sql, args = RulingModel(self._provider).objects.aggregate(Sum('id')).to_sql()
        self.assertEqual(sql, "SELECT *, SUM(id) as id__sum FROM test_model")

    def test_order_by(self):
        """ Test a simple order by clause """
        sql, args = RulingModel(self._provider).objects.order_by('id').to_sql()
        self.assertEqual(sql, "SELECT * FROM test_model ORDER BY id")

    def test_group_by(self):
        """ Test a simple group by clause """
        sql, args = RulingModel(self._provider).objects.group_by('id').to_sql()
        self.assertEqual(sql, "SELECT * FROM test_model GROUP BY id")

    def test_limit(self):
        """ Test the limit clause """
        sql, args = RulingModel(self._provider).objects.limit(10).to_sql()
        self.assertEqual(sql, "SELECT * FROM test_model LIMIT 10")
