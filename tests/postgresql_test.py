from __future__ import with_statement
import unittest2 as unittest
from contextlib import contextmanager
from mock import Mock, call
from api_spec import create_schema, _HTablesApiTest


class PostgresqlTest(_HTablesApiTest):

    CONNECTION_URI = 'postgresql://localhost/htables_test'

    def setUp(self):
        with self.db_session() as session:
            session.create_all()

    def tearDown(self):
        with self.db_session() as session:
            session.drop_all()

    @contextmanager
    def db_session(self):
        session_pool = self.schema.bind(self.CONNECTION_URI, debug=True)
        session = session_pool.get_session()
        try:
            yield session
        finally:
            session_pool.put_session(session)


def insert_spy(obj, attr_name):
    original_callable = getattr(obj, attr_name)
    spy = Mock(side_effect=original_callable)
    setattr(obj, attr_name, spy)
    return spy


class PostgresqlSessionTest(unittest.TestCase):

    CONNECTION_URI = PostgresqlTest.CONNECTION_URI

    def setUp(self):
        self.schema = create_schema()

    def _get_session_pool(self):
        return self.schema.bind(self.CONNECTION_URI, debug=True)

    def test_use_expired_connection(self):
        session_pool = self._get_session_pool()
        session = session_pool.get_session()
        session_pool.put_session(session)
        self.assertRaises(ValueError, session.commit)

    def test_lazy_session_does_not_initially_fetch_connection(self):
        session_pool = self._get_session_pool()
        session_pool = self.schema.bind(self.CONNECTION_URI, debug=True)
        spy = insert_spy(session_pool._conn_pool, 'getconn')
        session_pool.get_session(lazy=True)
        self.assertEqual(spy.mock_calls, [])

    def test_lazy_session_eventually_asks_for_connection(self):
        session_pool = self._get_session_pool()
        session_pool = self.schema.bind(self.CONNECTION_URI, debug=True)
        spy = insert_spy(session_pool._conn_pool, 'getconn')
        session = session_pool.get_session(lazy=True)
        session.commit()
        self.assertEqual(spy.mock_calls, [call()])
        self.addCleanup(session_pool.put_session, session)

    def test_lazy_session_with_no_connection_is_returned_ok(self):
        session_pool = self._get_session_pool()
        session_pool = self.schema.bind(self.CONNECTION_URI, debug=True)
        spy = insert_spy(session_pool._conn_pool, 'putconn')
        session = session_pool.get_session(lazy=True)
        session_pool.put_session(session)
        self.assertEqual(spy.mock_calls, [])

    def test_lazy_session_with_connection_puts_connection_back(self):
        session_pool = self._get_session_pool()
        session_pool = self.schema.bind(self.CONNECTION_URI, debug=True)
        spy = insert_spy(session_pool._conn_pool, 'putconn')
        session = session_pool.get_session(lazy=True)
        session.commit()
        conn = session._conn
        session_pool.put_session(session)
        self.assertEqual(spy.mock_calls, [call(conn)])
