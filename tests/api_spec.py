from __future__ import with_statement
from StringIO import StringIO
from contextlib import contextmanager
import warnings
from common import TestCase


class _HTablesApiTest(TestCase):

    def db_session(self):
        return self.db.session()

    def preSetUp(self):
        super(_HTablesApiTest, self).preSetUp()
        import htables
        self.db = self.create_db()
        with self.db_session() as session:
            session['person'].create_table()
            session.commit()

        def drop_all_tables():
            with self.db_session() as session:
                session['person'].drop_table()
                session.delete_all_blobs()
                session.commit()

        self.addCleanup(drop_all_tables)

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
        from htables import RowNotFound
        with self.db_session() as session:
            with self.assertRaises(RowNotFound):
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
            del person["k1"]  # remove value
            person["k2"] = "vX"  # change value
            # person["k3"] unchanged
            person["k4"] = "v4"  # add value
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

    def test_find_with_filter_and_missing_keys_in_rows(self):
        with self.db_session() as session:
            table = session['person']
            table.new(name='one', color='red').save()
            table.new(name='two').save()
            row1 = table.get(1)
            self.assertEqual(list(table.find(color='red')), [row1])

    def test_find_with_two_filters(self):
        with self.db_session() as session:
            table = session['person']
            table.new(name='one', color='red')
            table.new(name='two', color='red')
            table.new(name='two', color='blue')
            row2 = table.get(2)
            self.assertEqual(list(table.find(name='two', color='red')), [row2])

    def test_filter_with_funny_characters(self):
        with self.db_session() as session:
            table = session['person']
            name = 'adsfasdf!@#$%^&*()-=\\/"\'][{}><.,?`~ \n\t\fadsfasdf'
            table.new(name=name)
            row1 = table.get(1)
            self.assertEqual(list(table.find(name=name)), [row1])

    def test_find_first(self):
        with self.db_session() as session:
            table = session['person']
            table.new(name='one', color='blue').save()
            table.new(name='two', color='red').save()
            table.new(name='three', color='red').save()
            row2 = table.get(2)
            self.assertEqual(table.find_first(color='red'), row2)

    def test_find_first_no_results(self):
        from htables import RowNotFound
        with self.db_session() as session:
            table = session['person']
            self.assertRaises(RowNotFound, table.find_first, color='red')

    def test_find_single(self):
        with self.db_session() as session:
            table = session['person']
            table.new(name='one', color='blue').save()
            table.new(name='two', color='red').save()
            row2 = table.get(2)
            self.assertEqual(table.find_single(color='red'), row2)

    def test_find_single_with_more_results(self):
        from htables import MultipleRowsFound
        with self.db_session() as session:
            table = session['person']
            table.new(name='one', color='blue').save()
            table.new(name='two', color='red').save()
            table.new(name='three', color='red').save()
            self.assertRaises(MultipleRowsFound,
                              table.find_single, color='red')

    def test_find_single_with_no_results(self):
        from htables import RowNotFound
        with self.db_session() as session:
            table = session['person']
            self.assertRaises(RowNotFound, table.find_single, color='red')

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
                ''.join(db_file.iter_data())

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

    def test_table_access_with_missing_sql_table_raises_exception(self):
        from htables import MissingTable
        with self.db_session() as session:
            table = session['foo']
            with self.assertRaisesRegexp(MissingTable, r'^foo$') as e:
                table.new()

    def test_newly_created_table_holds_data(self):
        with self.db_session() as session:
            session['foo'].create_table()
            row = session['foo'].new()
            self.assertEqual(row.id, 1)

    def test_deleting_table_causes_exception(self):
        import htables
        with self.db_session() as session:
            session['foo'].create_table()
            session['foo'].drop_table()
            with self.assertRaises(htables.MissingTable):
                session['foo'].new()

    @contextmanager
    def expect_one_warning(self):
        if not hasattr(warnings, 'catch_warnings'):  # python < 2.6
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
        from nose import SkipTest
        raise SkipTest  # we don't have a schema any more
        PersonRow = self.schema['person']
        with self.db_session() as session:
            with self.expect_one_warning():
                session.table(PersonRow)


class _HTablesQueryApiTest(TestCase):

    def preSetUp(self):
        super(_HTablesQueryApiTest, self).preSetUp()
        import htables
        self.db = self.create_db()
        with self.db.session() as session:
            session['person'].create_table()
            session.commit()

        self.session = self.db.get_session()

        def cleanup():
            self.db.put_session(self.session)
            with self.db.session() as session:
                session['person'].drop_table()
                session.delete_all_blobs()
                session.commit()

        self.addCleanup(cleanup)

    def test_query_with_limit_2_returns_first_2_results(self):
        table = self.session['person']
        for c in range(4):
            table.new(name="row-%d" % c)
        results = list(table.query(limit=2))
        self.assertEqual(results, [{'name': "row-0"}, {'name': "row-1"}])

    def test_query_with_offset_2_and_no_limit_returns_last_2_results(self):
        table = self.session['person']
        for c in range(4):
            table.new(name="row-%d" % c)
        results = list(table.query(offset=2))
        self.assertEqual(results, [{'name': "row-2"}, {'name': "row-3"}])

    def test_query_with_limit_1_and_filtering_returns_first_match(self):
        table = self.session['person']
        for c in range(4):
            table.new(name="row-%d" % c,
                      parity="odd" if c%2 else "even")
        results = list(table.query(limit=1, where={'parity': "odd"}))
        self.assertEqual(results, [{'name': "row-1", 'parity': "odd"}])

    def test_order_by_string(self):
        table = self.session['person']
        table.new(name="row-1", letter='d')
        table.new(name="row-2", letter='c')
        table.new(name="row-3", letter='b')
        table.new(name="row-4", letter='a')
        results = list(table.query(order_by='letter'))
        self.assertEqual([row['name'] for row in results],
                         ['row-4', 'row-3', 'row-2', 'row-1'])

    def test_count_with_no_filter_returns_4(self):
        table = self.session['person']
        for c in range(4):
            table.new(name="row-%d" % c)
        results = table.query(count=True)
        self.assertEqual(results, 4)

    def test_count_with_filter_returns_2(self):
        table = self.session['person']
        for c in range(4):
            table.new(name="row-%d" % c,
                      parity="odd" if c%2 else "even")
        results = table.query(where={'parity': "odd"}, count=True)
        self.assertEqual(results, 2)

    def test_query_with_generic_regexp_returns_all(self):
        from htables import op
        table = self.session['person']
        for c in range(4):
            table.new(name="row-%d" % c,
                      parity="apple" if c%2 else "apples")
        results = table.query(where={'parity': op.RE('^ap')}, count=True)
        self.assertEqual(results, 4)

    def test_query_with_specific_regexp_returns_2(self):
        from htables import op
        table = self.session['person']
        for c in range(4):
            table.new(name="row-%d" % c,
                      parity="apple" if c%2 else "apples")
        results = table.query(where={'parity': op.RE('le$')}, count=True)
        self.assertEqual(results, 2)
