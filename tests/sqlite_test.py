from contextlib import contextmanager
import simplejson as json
from api_spec import _HTablesApiTest


class SqliteTest(_HTablesApiTest):

    def setUp(self):
        super(SqliteTest, self).setUp()
        import sqlite3
        self.conn = sqlite3.connect(':memory:')
        self.db_files = {}

    @contextmanager
    def db_session(self):
        import htables
        sqlite_session = htables.SqliteSession(
            self.schema, self.conn, self.db_files)
        sqlite_session.create_all()
        yield sqlite_session

    def _unpack_data(self, value):
        return json.loads(value)

    def _count_large_files(self, session):
        return len(self.db_files)

    def test_large_file_error(self):
        from nose import SkipTest
        raise SkipTest
