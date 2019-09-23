#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Copyright (c) 2019 Robert Abram - All Rights Reserved.
#
import unittest

from salty_orm.db.mysql_provider import MySQLDBConnection
from salty_orm.db.query import Q, QOper
from salty_orm.examples.models import RulingModel


class TestQueryStructure(unittest.TestCase):

    _provider = None

    def setUp(self) -> None:

        self._provider = MySQLDBConnection(testing=True)
        
    def test_qoper_equals(self):
        """ Test a Equals QOPer filter """
        sql, args = RulingModel(self._provider).objects.filter(Q('id', QOper.O_EQUAL, 12345)).to_sql()
        self.assertEqual(sql, "SELECT * FROM test_model WHERE id = ?")
        self.assertEqual(args, [12345])

    def test_qoper_like(self):
        """ Test a LIKE QOper filter """
        sql, args = RulingModel(self._provider).objects.filter(Q('name', QOper.O_LIKE, '%test%')).to_sql()
        self.assertEqual(sql, "SELECT * FROM test_model WHERE name LIKE ?")
        self.assertEqual(args, ['%test%'])

    def test_qoper_between(self):
        """ Test a Between QOper filter """
        sql, args = RulingModel(self._provider).objects.filter(Q('name', QOper.O_BETWEEN, 1, 20)).to_sql()
        self.assertEqual(sql, "SELECT * FROM test_model WHERE name BETWEEN ? AND ?")
        self.assertEqual(args, [1, 20])

    def test_qoper_in(self):
        """ test the In QOper filter """
        sql, args = RulingModel(self._provider).objects.filter(Q('name', QOper.O_IN, 'Jane', 'John', 'Patrick')). \
            to_sql()
        self.assertEqual(sql, "SELECT * FROM test_model WHERE name IN (?, ?, ?)")
        self.assertEqual(args, ['Jane', 'John', 'Patrick'])
