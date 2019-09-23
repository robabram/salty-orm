#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Copyright (c) 2019 Robert Abram - All Rights Reserved.
#
from datetime import datetime, timedelta
import unittest

from salty_orm.db.mysql_provider import MySQLDBConnection
from salty_orm.db.query import Q, QOper
from salty_orm.examples.models import RulingModel


class TestQueryStructure(unittest.TestCase):

    _provider = None

    def setUp(self) -> None:
        self._provider = MySQLDBConnection(testing=True)
        return super(TestQueryStructure, self).setUp()
        
    def test_qoper_equals(self):
        """ Test a Equals QOPer filter """
        sql, args = RulingModel(self._provider).objects.filter(Q('id', QOper.O_EQUAL, 12345)).to_sql()
        self.assertEqual(sql, "SELECT * FROM test_model WHERE id = %s")
        self.assertEqual(args, [12345])

    def test_qoper_like(self):
        """ Test a LIKE QOper filter """
        sql, args = RulingModel(self._provider).objects.filter(Q('name', QOper.O_LIKE, '%test%')).to_sql()
        self.assertEqual(sql, "SELECT * FROM test_model WHERE name LIKE %s")
        self.assertEqual(args, ['%test%'])

    def test_qoper_between(self):
        """ Test a Between QOper filter """
        sql, args = RulingModel(self._provider).objects.filter(Q('name', QOper.O_BETWEEN, 1, 20)).to_sql()
        self.assertEqual(sql, "SELECT * FROM test_model WHERE name BETWEEN %s AND %s")
        self.assertEqual(args, [1, 20])

        ts = datetime.now()

        sql, args = RulingModel(self._provider).objects.filter(
                        Q('created', QOper.O_BETWEEN, ts - timedelta(days=2), ts)).to_sql()
        self.assertEqual(sql, "SELECT * FROM test_model WHERE created BETWEEN \'%s\' AND \'%s\'")
        self.assertEqual(args, [ts - timedelta(days=2), ts])

    def test_qoper_in(self):
        """ test the In QOper filter """
        sql, args = RulingModel(self._provider).objects.filter(Q('name', QOper.O_IN, 'Jane', 'John', 'Patrick')).\
                                    to_sql()
        self.assertEqual(sql, "SELECT * FROM test_model WHERE name IN (%s, %s, %s)")
        self.assertEqual(args, ['Jane', 'John', 'Patrick'])
