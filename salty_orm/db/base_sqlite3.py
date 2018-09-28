#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Copyright (c) 2018 Robert Abram - All Rights Reserved.
#

import os
import sqlite3

from salty_orm.db.base_connection import BaseDBConnection, NotConnectedError, ConnectionFailedError, \
    ExecStatementFailedError, InvalidStatementError


def dict_factory(cursor, row):
    """
    Use for the sqlite connection row_factory function so we can lookup row data by field name.
    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class SqliteDBConnection(BaseDBConnection):
    """
    Used by the system service code base to connect to the sandtrap.db
    """

    _db_path = None  # Path to sqlite3 database
    provider = 'sqlite3'

    def __del__(self):
        self.db_close()

    def db_connect(self, alt_db_path=None) -> bool:
        """
        Connect to a local sqlite3 database
        :param alt_db_path: Alternate database path to use besides hardcoded path
        :return: True if connected otherwise False
        """
        db_path = self._db_path

        if alt_db_path:
            db_path = alt_db_path

        if os.path.exists(db_path):
            self._db_path = db_path
        else:
            raise FileNotFoundError('database path not found ({0})'.format(db_path))

        try:
            self._handle = sqlite3.connect(db_path)
            self._handle.row_factory = dict_factory
            self._connected = True
            return True

        except Exception as e:
            raise NotConnectedError("connection attempt to database failed")

    def db_close(self):
        """
        Disconnect from the local sqlite3 database if we are connected
        """

        if self._connected and self._handle:
            self._handle.close()

        self._connected = False
        self._handle = None

    def db_connected(self) -> bool:
        """
        Return the connection state
        :return: True if connected otherwise False
        """
        return self._connected

    def db_test_connection(self) -> bool:
        """
        Test the connection for connection errors with a simple Select statement
        :return: True if connection is good otherwise False, exception message
        """

        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")

        try:
            sql = 'SELECT * FROM sqlite_master LIMIT 1;'

            cursor = self._handle.cursor()
            cursor.execute(sql)
            cursor.close()

        except Exception as e:
            raise ConnectionFailedError(e)

        return True

    def db_exec(self, stmt: str, args: dict=None) -> bool:

        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")

        try:
            cursor = self._handle.cursor()
            cursor.execute(stmt, tuple(args.values()))
            cursor.close()
            self._handle.commit()

        except Exception as e:
            raise ExecStatementFailedError(e)

        return True

    def db_commit(self) -> bool:
        """
        Call database commit         
        """
        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")

        try:
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

        try:

            cursor = self._handle.cursor()
            cursor.execute(stmt, tuple(args.values()))

            data = cursor.fetchall()
            cursor.close()

            return data

        except Exception as e:
            raise ExecStatementFailedError(e)

    def db_exec_select_by_id_all(self, table: str, pk: int) -> dict:

        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")

        tmp_l = list()
        tmp_l.append(pk)

        return self.db_exec_select("SELECT * FROM {0} WHERE id = ?".format(table), tuple(tmp_l))

    def db_exec_commit(self, stmt: str, args: dict=None) -> int:
        """
        Execute sql statement and commit
        """

        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")

        if not stmt:
            raise InvalidStatementError('sql statement is missing')

        try:

            cursor = self._handle.cursor()
            cursor.execute(stmt, tuple(args.values()))
            lastrowid = cursor.lastrowid
            self._handle.commit()
            cursor.close()

            return lastrowid if lastrowid else 1

        except Exception as e:
            raise ExecStatementFailedError(e)

    def db_attach_database(self, alias: str, db_path: str=None) -> bool:
        """
        Attach a database to the current connection with the specified alias
        :param alias: alias of connected database
        :param db_path: path to database
        :return: True if attached, otherwise false
        """
        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")

        if not db_path or not os.path.exists(db_path):
            raise NotConnectedError("database path given to attach is invalid")

        stmt = "ATTACH DATABASE ? AS ?"
        args = { "db_path": db_path, "alias": db_path, }

        return self.db_exec(stmt, args)

    def db_detach_database(self, alias: str) -> bool:
        """
        Detach an attached database on the current connection
        :param alias: alias of connected database
        :return: True if attached, otherwise false
        """

        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")

        sql = 'DETACH DATABASE ?'
        args = { "alias": alias, }

        return self.db_exec(sql, args)

    def db_get_table_record_count(self, table: str):
        """ return the record count of a table """

        if self.db_connected() is False:
            raise NotConnectedError("not connected to a database")

        data = self.db_exec_select("SELECT COUNT(1) AS count FROM {0}".format(table))

        if data:
            return int(data['count'])


        return 0

    def db_get_record_info(self, fields, table: str, pk: int):
        """ get the id, created and modified fields of a table record """

        tmp_l = list()
        tmp_l.append(pk)

        data = self.db_exec_select("SELECT {0} FROM {1} WHERE id = ?".format(fields, table), tuple(tmp_l))

        if data:
            return data

        return None

    def db_get_all_info(self, table: str, fields: str=None):
        """ get the id, created and modified fields of a table """

        tmp_l = list()
        tmp_l.append(id)

        if fields:
            fields = '"id", "modified", {0}'.format(fields)
        else:
            fields = '"id", "modified"'

        data = self.db_exec_select("SELECT {0} FROM {1} ORDER BY id".format(fields, table))

        if data:
            return data

        return None

    def db_get_record_all(self, table: str, pk: str):
        """ get all the fields of a table record """

        tmp_l = list()
        tmp_l.append(pk)

        data = self.db_exec_select("SELECT * FROM {0} WHERE id = ?".format(table), tuple(tmp_l))

        if data:
            return data

        return None

