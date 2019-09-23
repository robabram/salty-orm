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

    def test_max_column(self):
        """ Test a maximum column select, with group by clause """
        sql, args = RulingModel(self._provider).objects.values_list('name').aggregate(Max('id')).\
                                group_by('name').to_sql()
        self.assertEqual(sql, "SELECT name, MAX (id) as id__max FROM test_model GROUP BY name")

    def test_min_column(self):
        """ Test a minimum column select, with group by clause """
        sql, args = RulingModel(self._provider).objects.values_list('name').aggregate(Min('id')).\
                                group_by('name').to_sql()
        self.assertEqual(sql, "SELECT name, MIN (id) as id__min FROM test_model GROUP BY name")

    def test_count_column(self):
        """ Test a count column select, with group by clause """
        sql, args = RulingModel(self._provider).objects.values_list('name').aggregate(Count('id')).\
                                group_by('name').to_sql()
        self.assertEqual(sql, "SELECT name, COUNT (id) as id__count FROM test_model GROUP BY name")

    def test_sum_column(self):
        """ Test a sum column select, with group by clause """
        sql, args = RulingModel(self._provider).objects.values_list('name').aggregate(Sum('id')).\
                                group_by('name').to_sql()
        self.assertEqual(sql, "SELECT name, SUM (id) as id__sum FROM test_model GROUP BY name")
