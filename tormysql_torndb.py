# coding: u8

from __future__ import unicode_literals

import logging

from tornado import ioloop, gen
from tornado.util import ObjectDict

import tormysql


class Connection(object):
    def __init__(self,
        host, port, database,
        user=None, password=None,
        max_connections=50,  # 最大同时打开的连接数
        idle_seconds=3600,  # 某个连接超过多少秒就关闭（mysql默认为8小时）
        wait_connection_timeout=10,  # 连接超时等待时间(连接池满时等待其他连接释放）
        charset='utf8',
    ):
        self.host = host
        self.port = port

        self.user = user
        self.password = password

        self.database = database

        self.max_connections = max_connections
        self.idle_seconds = idle_seconds
        self.wait_connection_timeout = wait_connection_timeout
        self.charset = charset

        self.pool = None
        ioloop.IOLoop(make_current=False).run_sync(self.reconnect)

    @gen.coroutine
    def __del__(self):
        yield self.close()

    @gen.coroutine
    def close(self):
        """Closes this database connection."""
        if self.pool is not None:
            yield self.pool.close()
            self.pool = None

    @gen.coroutine
    def reconnect(self):
        """Closes the existing database connection and re-opens it."""

        yield self.close()

        self.pool = tormysql.ConnectionPool(
            host=self.host,
            port=self.port,

            user=self.user,
            passwd=self.password,

            db=self.database,

            max_connections=self.max_connections,
            idle_seconds=self.idle_seconds,
            wait_connection_timeout=self.wait_connection_timeout,

            charset=self.charset,
        )

    @gen.coroutine
    def query(self, query, *args, **kwargs):
        """Returns a row list for the given query and args."""

        with (yield self.pool.Connection()) as conn:
            try:
                with conn.cursor() as cursor:
                    yield self._execute(cursor, query, args, kwargs)
            except:
                yield conn.rollback()
            else:
                yield conn.commit()

                column_names = [d[0] for d in cursor.description]
                datas = [ObjectDict(zip(column_names, row)) for row in cursor]
                raise gen.Return(datas)

    @gen.coroutine
    def get(self, query, *args, **kwargs):
        """Returns the (singular) row returned by the given query.

        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """

        rows = yield self.query(query, *args, **kwargs)
        if not rows:
            raise gen.Return(None)
        elif len(rows) > 1:
            raise Exception("Multiple rows returned by function get()")
        else:
            raise gen.Return(rows[0])

    # rowcount is a more reasonable default return value than lastrowid,
    # but for historical compatibility execute() must return lastrowid.
    @gen.coroutine
    def execute(self, query, *args, **kwargs):
        """Executes the given query, returning the lastrowid from the query."""

        return self.execute_lastrowid(query, *args, **kwargs)

    @gen.coroutine
    def execute_lastrowid(self, query, *args, **kwargs):
        """Executes the given query, returning the lastrowid from the query."""

        with (yield self.pool.Connection()) as conn:
            try:
                with conn.cursor() as cursor:
                    yield self._execute(cursor, query, args, kwargs)
            except:
                yield conn.rollback()
            else:
                yield conn.commit()
                raise gen.Return(cursor.lastrowid)

    @gen.coroutine
    def execute_rowcount(self, query, *args, **kwargs):
        """Executes the given query, returning the rowcount from the query."""

        with (yield self.pool.Connection()) as conn:
            try:
                with conn.cursor() as cursor:
                    yield self._execute(cursor, query, args, kwargs)
            except:
                yield conn.rollback()
            else:
                yield conn.commit()
                raise gen.Return(cursor.rowcount)

    @gen.coroutine
    def executemany(self, query, args):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """
        return self.executemany_lastrowid(query, args)

    @gen.coroutine
    def executemany_lastrowid(self, query, args):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """

        with (yield self.pool.Connection()) as conn:
            try:
                with conn.cursor() as cursor:
                    yield cursor.executemany(query, args)
            except:
                yield conn.rollback()
            else:
                yield conn.commit()
                raise gen.Return(cursor.lastrowid)

    @gen.coroutine
    def executemany_rowcount(self, query, args):
        """Executes the given query against all the given param sequences.

        We return the rowcount from the query.
        """

        with (yield self.pool.Connection()) as conn:
            try:
                with conn.cursor() as cursor:
                    yield cursor.executemany(query, args)
            except:
                yield conn.rollback()
            else:
                yield conn.commit()
                raise gen.Return(cursor.rowcount)

    update = delete = execute_rowcount
    updatemany = executemany_rowcount

    insert = execute_lastrowid
    insertmany = executemany_lastrowid

    @gen.coroutine
    def _execute(self, cursor, query, args, kwargs):
        try:
            yield cursor.execute(query, kwargs or args)
        except:
            logging.error("Error connecting to MySQL on %s", self.host)
            self.close()
            raise
