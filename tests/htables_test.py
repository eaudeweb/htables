from __future__ import with_statement
import unittest2 as unittest
from contextlib import contextmanager
from StringIO import StringIO
import warnings
from mock import Mock, call


def setUpModule(self):
    import htables; self.htables = htables
    self.json = htables.json
    self.schema = htables.Schema()
    self.PersonRow = self.schema.define_table('PersonRow', 'person')


class _HTablesApiTest(unittest.TestCase):

    def _unpack_data(self, value):
        return value

    def test_save(self):
        with self.db_session() as session:
            session['person'].new(hello="world").save()
            session.commit()

        with self.db_session() as session:
            cursor = session.conn.cursor()
            cursor.execute("SELECT * FROM person")
            [row] = list(cursor)
            self.assertEqual(self._unpack_data(row[1]), {u"hello": u"world"})

    def test_autoincrement_id(self):
        with self.db_session() as session:
            table = session['person']
            p1 = table.new()
            p2 = table.new()
            self.assertEqual(p1.id, 1)
            self.assertEqual(p2.id, 2)
            session.commit()

        with self.db_session() as session:
            cursor = session.conn.cursor()
            cursor.execute("SELECT * FROM person")
            [row1, row2] = list(cursor)
            self.assertEqual(row1[0], 1)
            self.assertEqual(row2[0], 2)

    def test_load(self):
        with self.db_session() as session:
            session['person'].new(hello="world").save()
            session.commit()

        with self.db_session() as session:
            person = session['person'].get(1)
            self.assertEqual(person, {"hello": "world"})

    def test_load_not_found(self):
        with self.db_session() as session:
            with self.assertRaises(KeyError) as e:
                session['person'].get(13)

    def test_load_all(self):
        with self.db_session() as session:
            session['person'].new(hello="world").save()
            session['person'].new(x="y").save()
            session.commit()

        with self.db_session() as session:
            all_persons = list(session['person'].find())
            self.assertEqual(len(all_persons), 2)
            self.assertEqual(all_persons[0], {'hello': "world"})
            self.assertEqual(all_persons[0].id, 1)
            self.assertEqual(all_persons[1], {'x': "y"})
            self.assertEqual(all_persons[1].id, 2)

    def test_update(self):
        with self.db_session() as session:
            session['person'].new(k1="v1", k2="v2", k3="v3").save()
            session.commit()

        with self.db_session() as session:
            person = session['person'].get(1)
            del person["k1"] # remove value
            person["k2"] = "vX" # change value
            # person["k3"] unchanged
            person["k4"] = "v4" # add value
            person.save()
            session.commit()

        with self.db_session() as session:
            person = session['person'].get(1)
            self.assertEqual(person, {"k2": "vX", "k3": "v3", "k4": "v4"})

    def test_save_from_row(self):
        with self.db_session() as session:
            session['person'].new(hello="world").save()
            session.commit()

        with self.db_session() as session:
            person = session['person'].get(1)
            person['hello'] = "George"
            person.save()
            session.commit()

        with self.db_session() as session:
            person = session['person'].get(1)
            self.assertEqual(person, {'hello': "George"})

    def test_table_row_factory(self):
        with self.db_session() as session:
            row = session['person'].new()
            self.assertEqual(row.id, 1)
            row['hello'] = 'world'
            row.save()
            session.commit()

        with self.db_session() as session:
            person = session['person'].get(1)
            self.assertEqual(person, {'hello': 'world'})

    def test_table_row_factory_with_dict_args(self):
        with self.db_session() as session:
            session['person'].new({'hello': 'world'}, a='b').save()
            session.commit()

        with self.db_session() as session:
            person = session['person'].get(1)
            self.assertEqual(person, {'hello': 'world', 'a': 'b'})

    def test_delete(self):
        with self.db_session() as session:
            session['person'].new(hello="world").save()
            session.commit()

        with self.db_session() as session:
            session['person'].get(1).delete()
            session.commit()

        with self.db_session() as session:
            cursor = session.conn.cursor()
            cursor.execute("SELECT * FROM person")
            self.assertEqual(list(cursor), [])

    def test_delete_from_row(self):
        with self.db_session() as session:
            session['person'].new(hello="world").save()
            session.commit()

        with self.db_session() as session:
            row = session['person'].get(1)
            row.delete()
            session.commit()

        with self.db_session() as session:
            cursor = session.conn.cursor()
            cursor.execute("SELECT * FROM person")
            self.assertEqual(list(cursor), [])

    def test_find_no_data(self):
        with self.db_session() as session:
            self.assertEqual(list(session['person'].find()), [])

    def test_find_all_rows(self):
        with self.db_session() as session:
            table = session['person']
            table.new(name='one').save()
            table.new(name='two').save()
            row1 = table.get(1)
            row2 = table.get(2)
            self.assertEqual(list(table.find()), [row1, row2])

    def test_find_with_filter(self):
        with self.db_session() as session:
            table = session['person']
            table.new(name='one', color='red').save()
            table.new(name='two', color='blue').save()
            table.new(name='three', color='red').save()
            row1 = table.get(1)
            row3 = table.get(3)
            self.assertEqual(list(table.find(color='red')), [row1, row3])

    def test_find_first(self):
        with self.db_session() as session:
            table = session['person']
            table.new(name='one', color='blue').save()
            table.new(name='two', color='red').save()
            table.new(name='three', color='red').save()
            row2 = table.get(2)
            self.assertEqual(table.find_first(color='red'), row2)

    def test_find_first_no_results(self):
        with self.db_session() as session:
            table = session['person']
            self.assertRaises(KeyError, table.find_first, color='red')

    def test_find_single(self):
        with self.db_session() as session:
            table = session['person']
            table.new(name='one', color='blue').save()
            table.new(name='two', color='red').save()
            row2 = table.get(2)
            self.assertEqual(table.find_single(color='red'), row2)

    def test_find_single_with_more_results(self):
        with self.db_session() as session:
            table = session['person']
            table.new(name='one', color='blue').save()
            table.new(name='two', color='red').save()
            table.new(name='three', color='red').save()
            row2 = table.get(2)
            self.assertRaises(ValueError, table.find_single, color='red')

    def test_find_single_with_no_results(self):
        with self.db_session() as session:
            table = session['person']
            self.assertRaises(KeyError, table.find_single, color='red')

    def test_large_file(self):
        with self.db_session() as session:
            db_file = session.get_db_file()
            db_file.save_from(StringIO("hello large data"))
            session.commit()
            db_file_id = db_file.id

        with self.db_session() as session:
            db_file = session.get_db_file(db_file_id)
            data = ''.join(db_file.iter_data())
            self.assertEqual(data, "hello large data")

    def test_large_file_error(self):
        import psycopg2
        with self.db_session() as session:
            db_file = session.get_db_file(13)
            with self.assertRaises(psycopg2.OperationalError):
                data = ''.join(db_file.iter_data())

        with self.db_session() as session:
            db_file = session.get_db_file(13)
            with self.assertRaises(psycopg2.OperationalError):
                db_file.save_from(StringIO("bla bla"))

    def _count_large_files(self, session):
        cursor = session.conn.cursor()
        cursor.execute("SELECT DISTINCT oid FROM pg_largeobject_metadata")
        return len(list(cursor))

    def test_remove_large_file(self):
        with self.db_session() as session:
            db_file = session.get_db_file()
            db_file.save_from(StringIO("hello large data"))
            session.commit()
            db_file_id = db_file.id

        with self.db_session() as session:
            session.del_db_file(db_file_id)
            session.commit()

        with self.db_session() as session:
            self.assertEqual(self._count_large_files(session), 0)

    def test_table_access(self):
        with self.db_session() as session:
            session['person'].new(hello="world").save()
            session.commit()

        with self.db_session() as session:
            table = session['person']
            self.assertEqual(table.get(1), {'hello': 'world'})

    def test_table_access_bad_name(self):
        with self.db_session() as session:
            self.assertRaises(KeyError, lambda: session['no-such-table'])

    @contextmanager
    def expect_one_warning(self):
        if not hasattr(warnings, 'catch_warnings'): # python < 2.6
            from nose import SkipTest
            raise SkipTest
        with warnings.catch_warnings(record=True) as warn_log:
            warnings.simplefilter('always')
            yield
            self.assertEqual(len(warn_log), 1)
            [warn] = warn_log
            self.assertTrue(issubclass(warn.category, DeprecationWarning))
            self.assertIn("deprecated", str(warn.message))

    def test_deprecation_table_save(self):
        with self.db_session() as session:
            with self.expect_one_warning():
                table = session['person']
                table.save(table.new())

    def test_deprecation_table_delete(self):
        with self.db_session() as session:
            with self.expect_one_warning():
                table = session['person']
                table.delete(table.new().id)

    def test_deprecation_session_save(self):
        with self.db_session() as session:
            with self.expect_one_warning():
                table = session['person']
                session.save(table.new())

    def test_deprecation_table_get_all(self):
        with self.db_session() as session:
            with self.expect_one_warning():
                session['person'].get_all()

    def test_deprecation_session_table(self):
        with self.db_session() as session:
            with self.expect_one_warning():
                session.table(PersonRow)


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
        session_pool = schema.bind(self.CONNECTION_URI, debug=True)
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

    def _get_session_pool(self):
        return schema.bind(self.CONNECTION_URI, debug=True)

    def test_use_expired_connection(self):
        session_pool = self._get_session_pool()
        session = session_pool.get_session()
        session_pool.put_session(session)
        self.assertRaises(ValueError, session.commit)

    def test_lazy_session_does_not_initially_fetch_connection(self):
        session_pool = self._get_session_pool()
        session_pool = schema.bind(self.CONNECTION_URI, debug=True)
        spy = insert_spy(session_pool._conn_pool, 'getconn')
        session = session_pool.get_session(lazy=True)
        self.assertEqual(spy.mock_calls, [])

    def test_lazy_session_eventually_asks_for_connection(self):
        session_pool = self._get_session_pool()
        session_pool = schema.bind(self.CONNECTION_URI, debug=True)
        spy = insert_spy(session_pool._conn_pool, 'getconn')
        session = session_pool.get_session(lazy=True)
        session.commit()
        self.assertEqual(spy.mock_calls, [call()])
        self.addCleanup(session_pool.put_session, session)

    def test_lazy_session_with_no_connection_is_returned_ok(self):
        session_pool = self._get_session_pool()
        session_pool = schema.bind(self.CONNECTION_URI, debug=True)
        spy = insert_spy(session_pool._conn_pool, 'putconn')
        session = session_pool.get_session(lazy=True)
        session_pool.put_session(session)
        self.assertEqual(spy.mock_calls, [])

    def test_lazy_session_with_connection_puts_connection_back(self):
        session_pool = self._get_session_pool()
        session_pool = schema.bind(self.CONNECTION_URI, debug=True)
        spy = insert_spy(session_pool._conn_pool, 'putconn')
        session = session_pool.get_session(lazy=True)
        session.commit()
        conn = session._conn
        session_pool.put_session(session)
        self.assertEqual(spy.mock_calls, [call(conn)])


class SqliteTest(_HTablesApiTest):

    def setUp(self):
        super(SqliteTest, self).setUp()
        import sqlite3
        self.conn = sqlite3.connect(':memory:')
        self.db_files = {}

    @contextmanager
    def db_session(self):
        sqlite_session = htables.SqliteSession(schema, self.conn, self.db_files)
        sqlite_session.create_all()
        yield sqlite_session

    def _unpack_data(self, value):
        return json.loads(value)

    def _count_large_files(self, session):
        return len(self.db_files)

    def test_large_file_error(self):
        from nose import SkipTest
        raise SkipTest
