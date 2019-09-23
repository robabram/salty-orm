#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Copyright (c) 2018 Robert Abram - All Rights Reserved.
#

import collections
import mysql.connector as mysql

from salty_orm.db.sqlite3_provider import SqliteDBConnection as BaseDBConnection
from salty_orm.db.base_provider import NotConnectedError, ExecStatementFailedError, InvalidStatementError


class MySQLDBConnection(BaseDBConnection):

    provider = 'mysql'
    _buffered = False
    placeholder = '%s'  # statement argument placeholder

    def db_connect(self, user=None, password=None, database=None, host=None, buffered=False) -> bool:
        """
        Connect to a local sqlite3 database
        :param alt_db_path: Alternate database path to use besides hardcoded path
        :return: True if connected otherwise False
        """

        self._buffered = buffered

        try:
            self._handle = mysql.connect(
                user=user,
                password=password,
                database=database,
                host=host,
                buffered=buffered,
            )

            self._connected = True
            return True

        except Exception as e:
            raise NotConnectedError("connection attempt to database failed")

    def db_exec(self, stmt: str, args: dict=None) -> bool:

        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")

        # Convert dict to list
        if args and isinstance(args, collections.Mapping):
            args = args.values()

        try:
            cursor = self._handle.cursor()
            cursor.execute(stmt, args)
            cursor.close()
            self._handle.commit()

        except Exception as e:
            raise ExecStatementFailedError(e)

        return True

    def db_exec_stmt(self, stmt: str, args: dict=None) -> dict:
        """
        Execute a select statement
        :param stmt: sql statement
        :param args: argument list
        :return: sqlite cursor or none
        """
        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")

        if not stmt:
            raise InvalidStatementError('sql statement is missing')

        # Convert dict to list
        if args and isinstance(args, collections.Mapping):
            args = args.values()

        try:

            cursor = self._handle.cursor()
            cursor.execute(stmt, args)

            data = cursor.fetchall()

            fields = list()

            if cursor.description is not None:

                for x in range(len(cursor.description)):
                    fields.append(cursor.description[x][0])

                new_data = list()

                for row in data:

                    d = dict()
                    for idx, col in enumerate(fields):
                        d[col] = row[idx]

                    new_data.append(d)

                data = new_data

            cursor.close()

            # TODO: If cursor.description is not None, convert row tuples to dict
            # https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-description.html

            return data

        except Exception as e:
            raise ExecStatementFailedError(e)

    def db_exec_commit(self, stmt, args: dict=None) -> int:
        """
        Execute sql statement and commit
        """

        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")

        if not stmt:
            raise InvalidStatementError('sql statement is missing')

        # Convert dict to list
        if args and isinstance(args, collections.Mapping):
            args = list(args.values())

        try:

            cursor = self._handle.cursor()
            cursor.execute(stmt, args)
            lastrowid = cursor.lastrowid
            self._handle.commit()
            cursor.close()

            return lastrowid if lastrowid else 1

        except Exception as e:
            raise ExecStatementFailedError(e)

    def db_commit(self) -> bool:
        super(MySQLDBConnection, self).db_commit()

    def db_attach_database(self, alias, db_path=None) -> bool:
        raise NotImplementedError()

    def db_detach_database(self, alias) -> bool:
        raise NotImplementedError()