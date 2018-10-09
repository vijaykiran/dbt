import abc

import dbt.clients.agate_helper
import dbt.exceptions

from dbt.adapters.base import BaseConnectionManager
from dbt.compat import abstractclassmethod


class SQLConnectionManager(BaseConnectionManager):
    """The default connection manager with some common SQL methods implemented.

    Methods to implement:
        - cancel_connection
        - get_status
    """
    @abc.abstractmethod
    def cancel_connection(connection):
        """Cancel the given connection.

        :param Connection connection: The connection to cancel.
        """
        raise dbt.exceptions.NotImplementedException(
            '`cancel_connection` is not implemented for this adapter!'
        )

    def cancel_open_connections(self):
        global connections_in_use

        for name, connection in connections_in_use.items():
            if name == 'master':
                continue

            self.cancel_connection(connection)
            yield name

    def add_query(self, sql, name=None, auto_begin=True, bindings=None,
                  abridge_sql_log=False):
        connection = self.get_connection(model_name)
        connection_name = connection.name

        if auto_begin and connection.transaction_open is False:
            self.begin(connection_name)

        logger.debug('Using {} connection "{}".'
                     .format(self.type(), connection_name))

        with self.exception_handler(sql, connection_name):
            if abridge_sql_log:
                logger.debug('On %s: %s....', connection_name, sql[0:512])
            else:
                logger.debug('On %s: %s', connection_name, sql)
            pre = time.time()

            cursor = connection.handle.cursor()
            cursor.execute(sql, bindings)

            logger.debug("SQL status: %s in %0.2f seconds",
                         self.get_status(cursor), (time.time() - pre))

            return connection, cursor

    @abstractclassmethod
    def get_status(cls, cursor):
        """Get the status of the cursor.

        :param cursor: A database handle to get status from
        :return: The current status
        :rtype: str
        """
        raise dbt.exceptions.NotImplementedException(
            '`get_status` is not implemented for this adapter!'
        )

    def execute(self, sql, name=None, auto_begin=False,
                fetch=False):
        self.get_connection(name)
        _, cursor = self.add_query(sql, name, auto_begin)
        status = self.get_status(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor)
        else:
            table = dbt.clients.agate_helper.empty_table()
        return status, table
