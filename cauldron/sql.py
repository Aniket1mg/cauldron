import time

from asyncio import coroutine
from contextlib import contextmanager
from functools import wraps
from enum import Enum
import aiopg
import asyncio
import logging

from aiopg import create_pool, Pool, Cursor

import psycopg2

_CursorType = Enum('CursorType', 'PLAIN, DICT, NAMEDTUPLE')


def dict_cursor(func):
    """
    Decorator that provides a dictionary cursor to the calling function

    Adds the cursor as the second argument to the calling functions

    Requires that the function being decorated is an instance of a class or object
    that yields a cursor from a get_cursor(cursor_type=CursorType.DICT) coroutine or provides such an object
    as the first argument in its signature

    Yields:
        A client-side dictionary cursor
    """

    @wraps(func)
    def wrapper(cls, *args, **kwargs):
        with (yield from cls.get_cursor(_CursorType.DICT)) as c:
            return (yield from func(cls, c, *args, **kwargs))

    return wrapper


def cursor(func):
    """
    Decorator that provides a cursor to the calling function

    Adds the cursor as the second argument to the calling functions

    Requires that the function being decorated is an instance of a class or object
    that yields a cursor from a get_cursor() coroutine or provides such an object
    as the first argument in its signature

    Yields:
        A client-side cursor
    """

    @wraps(func)
    def wrapper(cls, *args, **kwargs):
        with (yield from cls.get_cursor()) as c:
            return (yield from func(cls, c, *args, **kwargs))

    return wrapper


def nt_cursor(func):
    """
    Decorator that provides a namedtuple cursor to the calling function

    Adds the cursor as the second argument to the calling functions

    Requires that the function being decorated is an instance of a class or object
    that yields a cursor from a get_cursor(cursor_type=CursorType.NAMEDTUPLE) coroutine or provides such an object
    as the first argument in its signature

    Yields:
        A client-side namedtuple cursor
    """

    @wraps(func)
    def wrapper(cls, *args, **kwargs):
        with (yield from cls.get_cursor(_CursorType.NAMEDTUPLE)) as c:
            return (yield from func(cls, c, *args, **kwargs))

    return wrapper


def transaction(func):
    """
    Provides a transacted cursor which will run in autocommit=false mode

    For any exception the transaction will be rolled back.
    Requires that the function being decorated is an instance of a class or object
    that yields a cursor from a get_cursor(cursor_type=CursorType.NAMEDTUPLE) coroutine or provides such an object
    as the first argument in its signature

    Yields:
        A client-side transacted named cursor
    """

    @wraps(func)
    def wrapper(cls, *args, **kwargs):
        with (yield from cls.get_cursor(_CursorType.NAMEDTUPLE)) as c:
            try:
                yield from c.execute('BEGIN')
                result = (yield from func(cls, c, *args, **kwargs))
            except Exception:
                yield from c.execute('ROLLBACK')
            else:
                yield from c.execute('COMMIT')
                return result

    return wrapper


class PostgresStore:
    _pool = None
    _connection_params = {}
    _use_pool = None
    _insert_string = "insert into {} ({}) values ({}) returning *;"
    _bulk_insert_string = "insert into {} ({}) values"
    _update_string = "update {} set ({}) = ({}) where ({}) returning *;"
    _select_all_string_with_condition = "select * from {} where ({}) limit {} offset {};"
    _select_all_string = "select * from {} limit {} offset {};"
    _select_selective_column = "select {} from {} limit {} offset {};"
    _select_selective_column_with_condition = "select {} from {} where ({}) limit {} offset {};"
    _select_all_string_with_condition_and_order_by = "select * from {} where ({}) order by {} limit {} offset {};"
    _select_all_string_with_order_by = "select * from {} order by {} limit {} offset {};"
    _select_selective_column_with_order_by = "select {} from {} order by {} limit {} offset {};"
    _select_selective_column_with_condition_and_order_by = "select {} from {} where ({}) order by {} limit {} offset {};"
    _delete_query = "delete from {} where ({});"
    _count_query = "select count(*) from {};"
    _count_query_where = "select count(*) from {} where {};"
    _OR = ' or '
    _AND = ' and '
    _LPAREN = '('
    _RPAREN = ')'
    _WHERE_AND = '{} {} %s'
    _PLACEHOLDER = ' %s,'
    _COMMA = ', '
    _pool_pending = asyncio.Semaphore(1)
    _return_val = ' returning *;'

    TRANSACTION_STATUS = {
        0: 'TRANSACTION_STATUS_IDLE',
        1: 'TRANSACTION_STATUS_ACTIVE',
        2: 'TRANSACTION_STATUS_INTRANS',
        3: 'TRANSACTION_STATUS_INERROR',
        4: 'TRANSACTION_STATUS_UNKNOWN'
    }

    CONNECTION_STATUS = {
        0: 'STATUS_SETUP',
        1: 'STATUS_READY',
        2: 'STATUS_BEGIN',
        3: 'STATUS_SYNC',
        4: 'STATUS_ASYNC',
        5: 'STATUS_PREPARED'
    }

    _LOCK_TIMESTAMP = asyncio.Semaphore(1)
    _LOCK_COUNT = asyncio.Semaphore(1)

    _TOTAL_LOG_COUNTS = 0
    _LAST_LOG_TIMESTAMP = int(time.time())

    @classmethod
    def get_total_log_counts(cls):
        with(yield from cls._LOCK_COUNT):
            return cls._TOTAL_LOG_COUNTS

    @classmethod
    def update_total_log_count(cls):
        with(yield from cls._LOCK_COUNT):
            cls._TOTAL_LOG_COUNTS += 1

    @classmethod
    def get_last_log_timestamp(cls):
        with(yield from cls._LOCK_TIMESTAMP):
            return cls._LAST_LOG_TIMESTAMP

    @classmethod
    def update_last_log_timestamp(cls):
        with(yield from cls._LOCK_TIMESTAMP):
            cls._LAST_LOG_TIMESTAMP = int(time.time())

    @classmethod
    def connect(cls, database: str, user: str, password: str, host: str, port: int, *, use_pool: bool=True,
                enable_ssl: bool=False, minsize=1, maxsize=10, keepalives_idle=5, keepalives_interval=4, echo=False,
                refresh_period=-1, acquire_conn_wait_time=60,
                **kwargs):
        """
        Sets connection parameters
        For more information on the parameters that is accepts,
        see : http://www.postgresql.org/docs/9.2/static/libpq-connect.html
        """
        cls._connection_params['database'] = database
        cls._connection_params['user'] = user
        cls._connection_params['password'] = password
        cls._connection_params['host'] = host
        cls._connection_params['port'] = port
        cls._connection_params['sslmode'] = 'prefer' if enable_ssl else 'disable'
        cls._connection_params['minsize'] = minsize
        cls._connection_params['maxsize'] = maxsize
        cls._connection_params['keepalives_idle'] = keepalives_idle
        cls._connection_params['keepalives_interval'] = keepalives_interval
        cls._connection_params['echo'] = echo
        cls._connection_params.update(kwargs)
        cls._use_pool = use_pool
        cls.refresh_period = refresh_period
        cls._acquire_conn_wait_time = acquire_conn_wait_time or 60

    @classmethod
    def use_pool(cls, pool: Pool):
        """
        Sets an existing connection pool instead of using connect() to make one
        """
        cls._pool = pool

    @classmethod
    @coroutine
    def get_pool(cls) -> Pool:
        """
        Yields:
            existing db connection pool
        """
        if len(cls._connection_params) < 5:
            raise ConnectionError('Please call SQLStore.connect before calling this method')
        if not cls._pool:
            with (yield from cls._pool_pending):
                if not cls._pool:
                    cls._pool = yield from create_pool(**cls._connection_params)
                    asyncio.async(cls._periodic_cleansing())
        return cls._pool

    @classmethod
    @coroutine
    def _periodic_cleansing(cls):
        """
        Periodically cleanses idle connections in pool
        """
        if cls.refresh_period > 0:
            yield from asyncio.sleep(cls.refresh_period * 60)
            logging.getLogger().info("Clearing unused DB connections")
            yield from cls._pool.clear()
            asyncio.async(cls._periodic_cleansing())

    @classmethod
    @coroutine
    def get_cursor(cls, cursor_type=_CursorType.PLAIN, override_use_pool=True):
        """
        Yields: new client-side cursor from existing db connection pool
        """
        if cls._use_pool and override_use_pool:
            _connection_source = yield from cls.get_pool()
        else:
            dsn_str = "dbname='{}' user='{}' password='{}' host='{}'".format(cls._connection_params['database'], cls._connection_params['user'], cls._connection_params['password'], cls._connection_params['host'])
            _connection_source = yield from aiopg.connect(dsn_str)

        if cursor_type == _CursorType.NAMEDTUPLE:
            cursor_ft = psycopg2.extras.NamedTupleCursor
        elif cursor_type == _CursorType.DICT:
            cursor_ft = psycopg2.extras.DictCursor
        else:
            cursor_ft = None

        _cur = yield from cls.cursor_from_connection_source(_connection_source, cursor_ft)

        if not (cls._use_pool and override_use_pool):
            _cur = cursor_context_manager(_connection_source, _cur)
        return _cur

    @classmethod
    @coroutine
    def cursor_from_connection_source(cls, connection_source, cursor_ft: _CursorType):
        try:
            _cur = yield from asyncio.wait_for(connection_source.cursor(cursor_factory=cursor_ft), timeout=cls._acquire_conn_wait_time)
            return _cur
        except asyncio.TimeoutError:
            logging.getLogger().info('Could not acquire connection from pool in %s seconds', cls._acquire_conn_wait_time)
            asyncio.async(cls.log_connection_pool_state())
            raise asyncio.TimeoutError

    @classmethod
    @coroutine
    def log_connection_pool_state(cls):

        # Check if function was executed in last 10 mins
        last_run_time = yield from cls.get_last_log_timestamp()
        total_log_count = yield from cls.get_total_log_counts()
        if int(time.time()) - last_run_time < 60 * 10 and total_log_count > 0:
            return

        yield from cls.update_last_log_timestamp()
        yield from cls.update_total_log_count()

        current_pool = cls._pool
        used_connections = list(current_pool._used)
        free_connections = list(current_pool._free)
        log_message_arr = list()

        log_message_arr.append(
            'Total free connections {}. Total used connections {}'.format(len(free_connections), len(used_connections)))

        not_active_backend_process = {used_conn._conn.get_backend_pid(): used_conn for used_conn in used_connections}
        if not_active_backend_process:

            log_message_arr.append('There are not active connections in used pool, logging their details....')

            get_backend_process_details_query = "select pid, state, query, now() current_time, query_start, state_change from pg_stat_activity where pid in %s"
            all_pids = list(not_active_backend_process.keys())
            backend_process_details = yield from cls.run_query_on_separate_connection(get_backend_process_details_query,
                                                                                      (tuple(all_pids),),
                                                                                      _CursorType.NAMEDTUPLE)
            for process_detail in backend_process_details:
                connection_obj = not_active_backend_process[process_detail.pid]
                auto_commit = connection_obj.autocommit
                timeout = connection_obj.timeout
                cursor_factory = connection_obj.cursor_factory
                transaction_status = connection_obj._conn.get_transaction_status()
                transaction_status = cls.TRANSACTION_STATUS[
                    transaction_status] if transaction_status in cls.TRANSACTION_STATUS else transaction_status
                connection_status = connection_obj.status
                connection_status = cls.CONNECTION_STATUS[
                    connection_status] if connection_status in cls.CONNECTION_STATUS else connection_status

                query_last_executed = process_detail.query
                connnection_state_in_rds = process_detail.state
                last_query_start_time = process_detail.query_start
                last_state_change_time = process_detail.state_change
                now_time = process_detail.now_time

                log_message_arr.append('Backend process id {}, autocommit {}, timeout {}, cursor_factory {}, '
                                       'transaction_status {}, connection_status {}, connnection_state_in_rds {}, '
                                       'now_time {}, last_query_start_time {}, last_state_change_time {}, '
                                       'query_last_executed {}'.
                                       format(process_detail.pid, auto_commit, timeout, cursor_factory,
                                              transaction_status, connection_status, connnection_state_in_rds,
                                              now_time, last_query_start_time, last_state_change_time,
                                              query_last_executed))

        log_message = '###########'.join(log_message_arr)
        logging.getLogger().info(log_message)

    @classmethod
    @coroutine
    def run_query_on_separate_connection(cls, query, format_values: tuple = None, cursor_type=_CursorType.PLAIN):
        """
        Runs DB query on a new connection (does not use connections from cls._pool)
        """
        format_values = format_values or []
        with (yield from cls.get_cursor(cursor_type, False)) as cur:
            yield from cur.execute(query, format_values)
            return (yield from cur.fetchall())

    @classmethod
    @coroutine
    @cursor
    def count(cls, cur, table:str, where_keys: list=None):
        """
        gives the number of records in the table

        Args:
            table: a string indicating the name of the table

        Returns:
            an integer indicating the number of records in the table

        """

        if where_keys:
            where_clause, values = cls._get_where_clause_with_values(where_keys)
            query = cls._count_query_where.format(table, where_clause)
            q, t = query, values
        else:
            query = cls._count_query.format(table)
            q, t = query, ()
        yield from cur.execute(q, t)
        result = yield from cur.fetchone()
        return int(result[0])

    @classmethod
    @coroutine
    @nt_cursor
    def insert(cls, cur, table: str, values: dict):
        """
        Creates an insert statement with only chosen fields

        Args:
            table: a string indicating the name of the table
            values: a dict of fields and values to be inserted

        Returns:
            A 'Record' object with table columns as properties

        """
        keys = cls._COMMA.join(values.keys())
        value_place_holder = cls._PLACEHOLDER * len(values)
        query = cls._insert_string.format(table, keys, value_place_holder[:-1])
        yield from cur.execute(query, tuple(values.values()))
        return (yield from cur.fetchone())

    @classmethod
    @coroutine
    @nt_cursor
    def bulk_insert(cls, cur, table: str, records: list):
        """
        Creates an insert statement with only chosen fields for a set of entries

        Args:
        table: a string indicating the name of the table
        records: A list of dictionaries consisting of all the records to be inserted
        :Returns:
           A set 'Record' objects with table columns as properties
        """
        keys = cls._COMMA.join(records[0].keys())
        value_ordered = list()
        for record in records:
            value_ordered.append([record[key] for key in records[0]])
        value_place_holder = cls._LPAREN + (cls._PLACEHOLDER*len(records[0]))[:-1] + cls._RPAREN
        values = ','.join((yield from cur.mogrify(value_place_holder, tuple(rec))).decode("utf-8") for rec in value_ordered)
        yield from cur.execute(cls._bulk_insert_string.format(table, keys) + values + cls._return_val)
        return (yield from cur.fetchall())


    @classmethod
    @coroutine
    @nt_cursor
    def update(cls, cur, table: str, values: dict, where_keys: list) -> tuple:
        """
        Creates an update query with only chosen fields
        Supports only a single field where clause

        Args:
            table: a string indicating the name of the table
            values: a dict of fields and values to be inserted
            where_keys: list of dictionary
            example of where keys: [{'name':('>', 'cip'),'url':('=', 'cip.com'},{'type':{'<=', 'manufacturer'}}]
            where_clause will look like ((name>%s and url=%s) or (type <= %s))
            items within each dictionary get 'AND'-ed and dictionaries themselves get 'OR'-ed

        Returns:
            an integer indicating count of rows deleted

        """
        keys = cls._COMMA.join(values.keys())
        value_place_holder = cls._PLACEHOLDER * len(values)
        where_clause, where_values = cls._get_where_clause_with_values(where_keys)
        query = cls._update_string.format(table, keys, value_place_holder[:-1], where_clause)
        yield from cur.execute(query, (tuple(values.values()) + where_values))
        return (yield from cur.fetchall())

    @classmethod
    def _get_where_clause_with_values(cls, where_keys):
        values = []

        def make_and_query(ele: dict):
            and_query = cls._AND.join([cls._WHERE_AND.format(e[0], e[1][0]) for e in ele.items()])
            values.extend([val[1] for val in ele.values()])
            return cls._LPAREN + and_query + cls._RPAREN

        return cls._OR.join(map(make_and_query, where_keys)), tuple(values)

    @classmethod
    @coroutine
    @cursor
    def delete(cls, cur, table: str, where_keys: list):
        """
        Creates a delete query with where keys
        Supports multiple where clause with and or or both

        Args:
            table: a string indicating the name of the table
            where_keys: list of dictionary
            example of where keys: [{'name':('>', 'cip'),'url':('=', 'cip.com'},{'type':{'<=', 'manufacturer'}}]
            where_clause will look like ((name>%s and url=%s) or (type <= %s))
            items within each dictionary get 'AND'-ed and dictionaries themselves get 'OR'-ed

        Returns:
            an integer indicating count of rows deleted

        """
        where_clause, values = cls._get_where_clause_with_values(where_keys)
        query = cls._delete_query.format(table, where_clause)
        yield from cur.execute(query, values)
        return cur.rowcount

    @classmethod
    @coroutine
    @nt_cursor
    def select(cls, cur, table: str, order_by: str=None, columns: list=None, where_keys: list=None, limit=100,
               offset=0):
        """
        Creates a select query for selective columns with where keys
        Supports multiple where claus with and or or both

        Args:
            table: a string indicating the name of the table
            order_by: a string indicating column name to order the results on
            columns: list of columns to select from
            where_keys: list of dictionary
            limit: the limit on the number of results
            offset: offset on the results

            example of where keys: [{'name':('>', 'cip'),'url':('=', 'cip.com'},{'type':{'<=', 'manufacturer'}}]
            where_clause will look like ((name>%s and url=%s) or (type <= %s))
            items within each dictionary get 'AND'-ed and across dictionaries get 'OR'-ed

        Returns:
            A list of 'Record' object with table columns as properties

        """
        if columns:
            columns_string = cls._COMMA.join(columns)
            if where_keys:
                where_clause, values = cls._get_where_clause_with_values(where_keys)
                if order_by:
                    query = cls._select_selective_column_with_condition_and_order_by.format(columns_string, table, where_clause,
                                                                               order_by, limit, offset)
                else:
                    query = cls._select_selective_column_with_condition.format(columns_string, table, where_clause,
                                                                                limit, offset)
                q, t = query, values
            else:
                if order_by:
                    query = cls._select_selective_column_with_order_by.format(columns_string, table, order_by, limit, offset)
                else:
                    query = cls._select_selective_column.format(columns_string, table, limit, offset)
                q, t = query, ()
        else:
            if where_keys:
                where_clause, values = cls._get_where_clause_with_values(where_keys)
                if order_by:
                    query = cls._select_all_string_with_condition_and_order_by.format(table, where_clause, order_by, limit, offset)
                else:
                    query = cls._select_all_string_with_condition.format(table, where_clause, limit, offset)
                q, t = query, values
            else:
                if order_by:
                    query = cls._select_all_string_with_order_by.format(table, order_by, limit, offset)
                else:
                    query = cls._select_all_string.format(table, limit, offset)
                q, t = query, ()

        yield from cur.execute(q, t)
        return (yield from cur.fetchall())

    @classmethod
    @coroutine
    @nt_cursor
    def call_stored_procedure(cls, cur, procname, parameters=None, timeout=None):
        yield from cur.callproc(procname, parameters=parameters, timeout=timeout)
        return (yield from cur.fetchall())

    @classmethod
    @coroutine
    @cursor
    def mogrify(cls, cur, query, parameters=None):
        return cur.mogrify(query, parameters=parameters)

    @classmethod
    @coroutine
    @nt_cursor
    def raw_sql(cls, cur, query: str, values: tuple):
        """
        Run a raw sql query

        Args:
            query : query string to execute
            values : tuple of values to be used with the query

        Returns:
            result of query as list of named tuple

        """
        yield from cur.execute(query, values)
        return (yield from cur.fetchall())


@contextmanager
def cursor_context_manager(conn, cur):
    try:
        yield cur
    finally:
        cur._impl.close()
        conn.close()
