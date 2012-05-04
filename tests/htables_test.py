import unittest
from contextlib import contextmanager
from StringIO import StringIO
import json
import htables


schema = htables.Schema()
PersonRow = schema.define_table('PersonRow', 'person')


class _HTablesApiTest(unittest.TestCase):

    def _unpack_data(self, value):
        return value

    def test_save(self):
        with self.db_session() as session:
            session.save(PersonRow(hello="world"))
            session.commit()

        with self.db_session() as session:
            cursor = session.conn.cursor()
            cursor.execute("SELECT * FROM person")
            [row] = list(cursor)
            self.assertEqual(self._unpack_data(row[1]), {u"hello": u"world"})

    def test_autoincrement_id(self):
        with self.db_session() as session:
            p1 = PersonRow()
            p2 = PersonRow()
            session.save(p1)
            session.save(p2)
            session.commit()
            self.assertEqual(p1.id, 1)
            self.assertEqual(p2.id, 2)

        with self.db_session() as session:
            cursor = session.conn.cursor()
            cursor.execute("SELECT * FROM person")
            [row1, row2] = list(cursor)
            self.assertEqual(row1[0], 1)
            self.assertEqual(row2[0], 2)

    def test_load(self):
        with self.db_session() as session:
            session.save(PersonRow(hello="world"))
            session.commit()

        with self.db_session() as session:
            person = session.table(PersonRow).get(1)
            self.assertEqual(person, {"hello": "world"})

    def test_load_not_found(self):
        with self.db_session() as session:
            with self.assertRaises(KeyError) as e:
                session.table(PersonRow).get(13)

    def test_load_all(self):
        with self.db_session() as session:
            session.save(PersonRow(hello="world"))
            session.save(PersonRow(x="y"))
            session.commit()

        with self.db_session() as session:
            all_persons = list(session.table(PersonRow).get_all())
            self.assertEqual(len(all_persons), 2)
            self.assertEqual(all_persons[0], {'hello': "world"})
            self.assertEqual(all_persons[0].id, 1)
            self.assertEqual(all_persons[1], {'x': "y"})
            self.assertEqual(all_persons[1].id, 2)

    def test_update(self):
        with self.db_session() as session:
            session.save(PersonRow(k1="v1", k2="v2", k3="v3"))
            session.commit()

        with self.db_session() as session:
            person = session.table(PersonRow).get(1)
            del person["k1"] # remove value
            person["k2"] = "vX" # change value
            # person["k3"] unchanged
            person["k4"] = "v4" # add value
            session.save(person)
            session.commit()

        with self.db_session() as session:
            person = session.table(PersonRow).get(1)
            self.assertEqual(person, {"k2": "vX", "k3": "v3", "k4": "v4"})

    def test_delete(self):
        with self.db_session() as session:
            session.save(PersonRow(hello="world"))
            session.commit()

        with self.db_session() as session:
            session.table(PersonRow).delete(1)
            session.commit()

        with self.db_session() as session:
            cursor = session.conn.cursor()
            cursor.execute("SELECT * FROM person")
            self.assertEqual(list(cursor), [])

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


def skip_me(self):
    from nose import SkipTest
    raise SkipTest

for name in ['test_large_file_error']:
    setattr(SqliteTest, name, skip_me)
