import abc
import multiprocessing

import six

import dbt.exceptions
import dbt.flags
from dbt.compat import abstractclassmethod
from dbt.contracts.connection import Connection
from dbt.logger import GLOBAL_LOGGER as logger


@six.add_metaclass(abc.ABCMeta)
class BaseConnectionManager(object):
    """Methods to implement:
        - exception_handler
        - cancel_open
        - open
        - begin
        - commit
        - execute

    You must also set the 'TYPE' class attribute with a class-unique constant
    string.
    """
    TYPE = NotImplemented

    def __init__(self, profile):
        self.profile = profile
        self.in_use = {}
        self.available = []
        self.lock = multiprocessing.Lock()

    @abc.abstractmethod
    def exception_handler(self, sql, connection_name='master'):
        """Create a context manager that handles exceptions caused by database
        interactions.

        :param str sql: The SQL string that the block inside the context
            manager is executing.
        :param str connection_name: The name of the connection being used
        :return: A context manager that handles exceptions raised by the
            underlying database.
        """
        raise dbt.exceptions.NotImplementedException(
            '`exception_handler` is not implemented for this adapter!')

    def get(self, name=None, recache_if_missing=True):
        if name is None:
            # if a name isn't specified, we'll re-use a single handle
            # named 'master'
            name = 'master'

        if self.in_use.get(name):
            return self.in_use.get(name)

        if not recache_if_missing:
            raise dbt.exceptions.InternalException(
                'Tried to get a connection "{}" which does not exist '
                '(recache_if_missing is off).'.format(name))

        logger.debug('Acquiring new {} connection "{}".'
                     .format(self.TYPE, name))

        connection = self.acquire(name)
        self.in_use[name] = connection

        return self.get(name)

    @abc.abstractmethod
    def cancel_open(self):
        """Cancel all open connections on the adapter. (passable)"""
        raise dbt.exceptions.NotImplementedException(
            '`cancel_open` is not implemented for this adapter!'
        )

    @abstractclassmethod
    def open(cls, connection):
        """Open a connection on the adapter.

        This may mutate the given connection (in particular, its state and its
        handle).

        :param Connection connection: A connection object to open.
        :return: A connection with a handle attached and an 'open' state.
        :rtype: Connection
        """
        raise dbt.exceptions.NotImplementedException(
            '`open` is not implemented for this adapter!'
        )

    def total_allocated(self):
        return len(self.in_use) + len(self.available)

    def acquire(self, name):

        # we add a magic number, 2 because there are overhead connections,
        # one for pre- and post-run hooks and other misc operations that occur
        # before the run starts, and one for integration tests.
        max_connections = self.profile.threads + 2

        with self.lock:
            num_allocated = self.total_allocated()

            if self.available:
                logger.debug('Re-using an available connection from the pool.')
                to_return = self.available.pop()
                to_return.name = name
                return to_return

            elif num_allocated >= max_connections:
                raise dbt.exceptions.InternalException(
                    'Tried to request a new connection "{}" but '
                    'the maximum number of connections are already '
                    'allocated!'.format(name))

            logger.debug('Opening a new connection ({} currently allocated)'
                         .format(num_allocated))

            result = Connection(
                type=self.TYPE,
                name=name,
                state='init',
                transaction_open=False,
                handle=None,
                credentials=self.profile.credentials
            )

            return self.open(result)

    def release(self, name):
        with self.lock:

            if name not in self.in_use:
                return

            to_release = self.get(name, recache_if_missing=False)

            if to_release.state == 'open':

                if to_release.transaction_open is True:
                    self.rollback(to_release)

                to_release.name = None
                self.available.append(to_release)
            else:
                self.close(to_release)

            del self.in_use[name]

    def cleanup_all(self):
        with self.lock:
            for name, connection in self.in_use.items():
                if connection.get('state') != 'closed':
                    logger.debug("Connection '{}' was left open."
                                 .format(name))
                else:
                    logger.debug("Connection '{}' was properly closed."
                                 .format(name))

            conns_in_use = list(self.in_use.values())
            for conn in conns_in_use + self.available:
                self.close(conn)

            # garbage collect these connections
            self.in_use.clear()
            self.available = []

    def reload(self, connection):
        return self.get(connection.name)

    @abc.abstractmethod
    def begin(self, name):
        """Begin a transaction. (passable)

        :param str name: The name of the connection to use.
        """
        raise dbt.exceptions.NotImplementedException(
            '`begin` is not implemented for this adapter!'
        )

    def get_if_exists(self, name):
        if name is None:
            name = 'master'

        if self.in_use.get(name) is None:
            return

        return self.get(name, False)

    @abc.abstractmethod
    def commit(self, connection):
        """Commit a transaction. (passable)

        :param str name: The name of the connection to use.
        """
        raise dbt.exceptions.NotImplementedException(
            '`commit` is not implemented for this adapter!'
        )

    def rollback(self, connection):
        if dbt.flags.STRICT_MODE:
            Connection(**connection)

        connection = self.reload(connection)

        if connection.transaction_open is False:
            raise dbt.exceptions.InternalException(
                'Tried to rollback transaction on connection "{}", but '
                'it does not have one open!'.format(connection.name))

        logger.debug('On {}: ROLLBACK'.format(connection.name))
        connection.handle.rollback()

        connection.transaction_open = False
        self.in_use[connection.name] = connection

        return connection

    @classmethod
    def close(cls, connection):
        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, Connection)

        # On windows, sometimes connection handles don't have a close() attr.
        if hasattr(connection.handle, 'close'):
            connection.handle.close()

        connection.state = 'closed'

        return connection

    def commit_if_has_connection(self, name):
        """If the named connection exists, commit the current transaction.

        :param str name: The name of the connection to use.
        """
        connection = self.get_if_exists(name)
        if connection:
            self.commit(connection)

    def clear_transaction(self, conn_name='master'):
        conn = self.begin(conn_name)
        self.commit(conn)
        return conn_name

    @abc.abstractmethod
    def execute(self, sql, name=None, auto_begin=False, fetch=False):
        """Execute the given SQL.

        :param str sql: The sql to execute.
        :param Optional[str] name: The name to use for the connection.
        :param bool auto_begin: If set, and dbt is not currently inside a
            transaction, automatically begin one.
        :param bool fetch: If set, fetch results.
        :return: A tuple of the status and the results (empty if fetch=False).
        :rtype: Tuple[str, agate.Table]
        """
        raise dbt.exceptions.NotImplementedException(
            '`execute` is not implemented for this adapter!'
        )
