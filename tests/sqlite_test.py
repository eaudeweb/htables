from __future__ import with_statement
import unittest2 as unittest
from contextlib import contextmanager
import sqlite3
import tempfile
import simplejson as json
from path import path
from api_spec import _HTablesApiTest, create_schema


class SqliteDB(object):

    def __init__(self, uri, schema):
        if uri == ':memory:':
            self._single_connection = sqlite3.connect(uri)
        else:
            self._single_connection = None
            self._uri = uri

        self._files = {}
        self.schema = schema

    def _connect(self):
        if self._single_connection is None:
            return sqlite3.connect(self._uri)
        else:
            return self._single_connection

    def get_session(self):
        import htables
        session = htables.SqliteSession(
            self.schema, self._connect(), self._files)
        return session

    def put_session(self, session):
        pass


@contextmanager
def db_session(pool):
    session = pool.get_session()
    try:
        yield session
    finally:
        pool.put_session(session)


class SqliteTest(_HTablesApiTest):

    def setUp(self):
        self.session_pool = SqliteDB(':memory:', schema=self.schema)

        with self.db_session() as session:
            session.create_all()

    def db_session(self):
        return db_session(self.session_pool)

    def _unpack_data(self, value):
        return json.loads(value)

    def _count_large_files(self, session):
        return len(self.session_pool._files)

    def test_large_file_error(self):
        from nose import SkipTest
        raise SkipTest


class SqliteSessionTest(unittest.TestCase):

    def setUp(self):
        self.schema = create_schema()

    def assert_consecutive_sessions_access_same_database(self, db):
        with db_session(db) as session:
            session.create_all()
            session['person'].new(name="Joe")
            session.commit()

        with db_session(db) as session:
            self.assertEqual(list(session['person'].find()), [{'name': "Joe"}])

    def test_memory_consecutive_access(self):
        db = SqliteDB(':memory:', schema=self.schema)
        self.assert_consecutive_sessions_access_same_database(db)

    def create_filesystem_db(self):
        self.tmp = path(tempfile.mkdtemp())
        self.addCleanup(self.tmp.rmtree)
        return SqliteDB(self.tmp / 'db.sqlite', schema=self.schema)

    def test_filesystem_consecutive_access(self):
        db = self.create_filesystem_db()
        self.assert_consecutive_sessions_access_same_database(db)

    def test_filesystem_different_connections(self):
        db = self.create_filesystem_db()
        with db_session(db) as session1:
            session1.create_all()
            session1.commit()
            with db_session(db) as session2:
                session1['person'].new(name="Joe")
                self.assertEqual(list(session2['person'].find()), [])
