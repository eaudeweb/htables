from __future__ import with_statement
import unittest2 as unittest
from contextlib import contextmanager
from mock import Mock, call
from api_spec import _HTablesApiTest


CONNECTION_URI = 'postgresql://localhost/htables_test'


class PostgresqlTest(_HTablesApiTest):

    def create_db(self):
        import htables
        return htables.PostgresqlDB(CONNECTION_URI, debug=True)


def insert_spy(obj, attr_name):
    original_callable = getattr(obj, attr_name)
    spy = Mock(side_effect=original_callable)
    setattr(obj, attr_name, spy)
    return spy


class PostgresqlSessionTest(unittest.TestCase):

    def get_db(self):
        import htables
        return htables.PostgresqlDB(CONNECTION_URI, debug=True)

    def test_use_expired_connection(self):
        db = self.get_db()
        session = db.get_session()
        db.put_session(session)
        self.assertRaises(RuntimeError, session.commit)

    def test_lazy_session_does_not_initially_fetch_connection(self):
        db = self.get_db()
        spy = insert_spy(db._conn_pool, 'getconn')
        db.get_session(lazy=True)
        self.assertEqual(spy.mock_calls, [])

    def test_lazy_session_eventually_asks_for_connection(self):
        db = self.get_db()
        spy = insert_spy(db._conn_pool, 'getconn')
        session = db.get_session(lazy=True)
        session.commit()
        self.assertEqual(spy.mock_calls, [call()])
        self.addCleanup(db.put_session, session)

    def test_lazy_session_with_no_connection_is_returned_ok(self):
        db = self.get_db()
        spy = insert_spy(db._conn_pool, 'putconn')
        session = db.get_session(lazy=True)
        db.put_session(session)
        self.assertEqual(spy.mock_calls, [])

    def test_lazy_session_with_connection_puts_connection_back(self):
        db = self.get_db()
        spy = insert_spy(db._conn_pool, 'putconn')
        session = db.get_session(lazy=True)
        session.commit()
        conn = session._conn
        db.put_session(session)
        self.assertEqual(spy.mock_calls, [call(conn)])
