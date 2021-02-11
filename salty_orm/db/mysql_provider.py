#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Copyright (c) 2018 Robert Abram - All Rights Reserved.
#

import collections
import MySQLdb as mysql
from typing import Union

from salty_orm.db.sqlite3_provider import SqliteDBConnection as BaseDBConnection
from salty_orm.db.base_provider import NotConnectedError, ExecStatementFailedError, InvalidStatementError


class MySQLDBConnection(BaseDBConnection):

    provider = 'mysql'
    _buffered = False
    placeholder = '%s'  # statement argument placeholder

    def db_connect(self, user=None, password=None, database=None, host=None, **kwargs) -> bool:
        """
        Connect to a local MySQL/Mariadb database.
        :param user: User name to connect to database with.
        :param password: Password to connect to database with.
        :param database: Database name.
        :param host: Database host name or ip address.
        :param kwargs: Additonal named arguments to pass to MySQLdb connection.
        :return: True if connected otherwise False.
        """
        try:
            self._handle = mysql.connect(user=user, passwd=password, db=database, host=host, **kwargs)
            self._connected = True
            return True
        except Exception as e:
            raise NotConnectedError("Error: Connection attempt to database failed. \n{0}".format(e))

    def db_cursor(self):
        """
        Return a mysql connection cursor object
        :return: Cursor object """
        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")

        cursor = self._handle.cursor()
        return cursor

    def db_callproc(self, proc: str, args: Union[dict, list] = None) -> dict:
        """
        Call a database stored procedure.
        :param proc: procedure name
        :param args: arguments to procedure.
        :return: dict
        """
        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")
        if not proc:
            raise ValueError('Procedure name must not be empty.')

        # Convert dict to list
        if args and isinstance(args, collections.abc.Mapping):
            args = args.values()

        try:

            cursor = self._handle.cursor()
            cursor.callproc(proc, args)

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
            return data
        except Exception as e:
            raise ExecStatementFailedError(e)

    def db_exec(self, stmt: str, args: Union[dict, list] = None) -> bool:
        """
        Execute a SQL Statement that returns no data.
        :param stmt: SQL Statement to execute.
        :param args: List or dictionary of parameterized arguments.
        :return: True if successful, otherwise False.
        """
        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")

        # Convert dict to list
        if args and isinstance(args, collections.abc.Mapping):
            args = args.values()

        try:
            cursor = self._handle.cursor()
            cursor.execute(stmt, args)
            cursor.close()
            self._handle.commit()

        except Exception as e:
            raise ExecStatementFailedError(e)

        return True

    def db_exec_stmt(self, stmt: str, args: Union[dict, list] = None) -> dict:
        """
        Execute a statement that returns data.
        :param stmt: SQL statement
        :param args: List or dictionary of parameterized arguments.
        :return: cursor or none
        """
        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")

        if not stmt:
            raise InvalidStatementError('sql statement is missing')

        # Convert dict to list
        if args and isinstance(args, collections.abc.Mapping):
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

    def db_exec_commit(self, stmt, args: Union[dict, list] = None) -> int:
        """
        Execute sql statement and commit.
        :param stmt: SQL statement
        :param args: List or dictionary of parameterized arguments.
        :return: Last row id or 1.
        """

        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")

        if not stmt:
            raise InvalidStatementError('sql statement is missing')

        # Convert dict to list
        if args and isinstance(args, collections.abc.Mapping):
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
        return super(MySQLDBConnection, self).db_commit()

    def db_attach_database(self, alias, db_path=None) -> bool:
        raise NotImplementedError()

    def db_detach_database(self, alias) -> bool:
        raise NotImplementedError()
