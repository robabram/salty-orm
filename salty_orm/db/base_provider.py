#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Copyright (c) 2018 Robert Abram - All Rights Reserved.
#

class ConnectionError(Exception):
    """Base class for exceptions in this module."""

    message = None

    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return self.message


class NotConnectedError(ConnectionError):
    pass


class ConnectionFailedError(ConnectionError):
    pass


class InvalidStatementError(ConnectionError):
    pass


class ExecStatementFailedError(ConnectionError):
    pass


class BaseDBConnection(object):
    """
    Base Connection Object
    """
    _handle = None  # Sqlite3 connection handle
    _connected = False  # Are we connected to the database
    provider = None  # Database provider name
    placeholder = '?'  # statement argument placeholder

    testing = False  # unit testing flag.

    def __init__(self, testing: bool = False):
        self.testing = testing

    def __del__(self):
        pass

    def get_name(self) -> str:
        """
        :return: module name
        """
        # return self._name
        return type(self).__name__

    def db_connect(self, alt_db_path: str = None) -> bool:

        raise NotImplementedError()

    def db_close(self):
        """
        Disconnect from the local sqlite3 database if we are connected
        """
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
        raise NotImplementedError()

    def db_exec(self, sql: str, args: dict=None) -> bool:

        raise NotImplementedError()

    def db_commit(self) -> bool:
        """
        Call database commit         
        """
        raise NotImplementedError()

    def db_exec_stmt(self, stmt: str, args: dict=None) -> dict:
        """
        Execute a database statement
        :param stmt: database statement
        :param args: argument dict
        :return: dict
        """
        raise NotImplementedError()

    def db_exec_select_by_id_all(self, table: str, pk: int) -> dict:
        raise NotImplementedError()

    def db_exec_commit(self, stmt: str, args: dict=None) -> int:
        """
        Execute database statement and commit
        """
        raise NotImplementedError()

    def db_get_table_record_count(self, table: str) -> int:
        """ return the record count of a table """
        raise NotImplementedError()

    def db_get_record_info(self, fields, table: str, pk: int) -> dict:
        """ get the id, created and modified fields of a table record """
        raise NotImplementedError()

    def db_get_all_info(self, table: str, fields: dict=None) -> dict:
        """ get the id, created and modified fields of a table """
        raise NotImplementedError()

    def db_get_record_all(self, table: str, pk: int) -> dict:
        """ get all the fields of a table record """
        raise NotImplementedError()
