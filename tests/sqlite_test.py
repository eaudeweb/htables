from __future__ import with_statement
from contextlib import contextmanager
import sqlite3
import tempfile
import simplejson as json
from common import TestCase
import api_spec


@contextmanager
def db_session(pool):
    session = pool.get_session()
    try:
        yield session
    finally:
        pool.put_session(session)


class SqliteApiTest(api_spec._HTablesApiTest):

    def create_db(self):
        import htables
        temp_db = tempfile.NamedTemporaryFile()
        self.addCleanup(temp_db.close)
        return htables.SqliteDB(temp_db.name)

    def _unpack_data(self, value):
        return json.loads(value)

    def _count_large_files(self, session):
        return len(self.db._files)

    def test_large_file_error(self):
        from nose import SkipTest
        raise SkipTest

    def test_large_file(self):
        from nose import SkipTest
        raise SkipTest

    def test_remove_large_file(self):
        from nose import SkipTest
        raise SkipTest


class SqliteQueryApiTest(api_spec._HTablesQueryApiTest):

    def create_db(self):
        import htables
        temp_db = tempfile.NamedTemporaryFile()
        self.addCleanup(temp_db.close)
        return htables.SqliteDB(temp_db.name)


class SqliteApiMemoryTest(api_spec._HTablesApiTest):

    def create_db(self):
        import htables
        return htables.SqliteDB(':memory:')

    def _unpack_data(self, value):
        return json.loads(value)

    def _count_large_files(self, session):
        return len(self.db._files)

    def test_large_file_error(self):
        from nose import SkipTest
        raise SkipTest


class SqliteQueryApiMemoryTest(api_spec._HTablesQueryApiTest):

    def create_db(self):
        import htables
        return htables.SqliteDB(':memory:')


class SqliteSessionTest(TestCase):

    def setUp(self):
        import htables
        self.schema = htables.Schema(['person'])

    def assert_consecutive_sessions_access_same_database(self, db):
        with db_session(db) as session:
            session.create_all()
            session['person'].new(name="Joe")
            session.commit()

        with db_session(db) as session:
            self.assertEqual(list(session['person'].find()), [{'name': "Joe"}])

    def test_memory_consecutive_access(self):
        import htables
        db = htables.SqliteDB(':memory:', schema=self.schema)
        self.assert_consecutive_sessions_access_same_database(db)

    def create_filesystem_db(self):
        import htables
        db_path = self.tmpdir() / 'db.sqlite'
        return htables.SqliteDB(db_path, schema=self.schema)

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

    def test_filesystem_closes_connection(self):
        db = self.create_filesystem_db()
        with db_session(db) as session:
            connection = session.conn
        self.assertRaises(sqlite3.ProgrammingError, connection.cursor)
        self.assertRaises(RuntimeError, lambda: session.conn)

    def test_filesystem_db_does_not_support_blobs(self):
        import htables
        db = self.create_filesystem_db()
        with db_session(db) as session:
            self.assertRaises(htables.BlobsNotSupported, session.get_db_file)
