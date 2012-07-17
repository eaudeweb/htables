from __future__ import with_statement
from contextlib import contextmanager
import sqlite3
import simplejson as json
from api_spec import _HTablesApiTest


class SqliteDB(object):

    def __init__(self, uri, schema):
        self._single_connection = sqlite3.connect(uri)
        self._files = {}
        self.schema = schema

    def _connect(self):
        return self._single_connection

    def get_session(self):
        import htables
        session = htables.SqliteSession(
            self.schema, self._connect(), self._files)
        return session

    def put_session(self, session):
        pass


class SqliteTest(_HTablesApiTest):

    def setUp(self):
        self.session_pool = SqliteDB(':memory:', schema=self.schema)

        with self.db_session() as session:
            session.create_all()

    @contextmanager
    def db_session(self):
        session = self.session_pool.get_session()
        try:
            yield session
        finally:
            self.session_pool.put_session(session)

    def _unpack_data(self, value):
        return json.loads(value)

    def _count_large_files(self, session):
        return len(self.session_pool._files)

    def test_large_file_error(self):
        from nose import SkipTest
        raise SkipTest
