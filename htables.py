try:
    import simplejson as json
except ImportError:
    import json
import random
import StringIO
import warnings
import re
from contextlib import contextmanager


class BlobsNotSupported(Exception):
    """ This database does not support blobs. """


class RowNotFound(KeyError):
    """ No row matching search criteria. """
    # TODO don't subclass from KeyError


class MultipleRowsFound(ValueError):
    """ Multiple rows matching search critera. """
    # TODO don't subclass ValueError


class MissingTable(RuntimeError):
    """ Table missing from database. """


COPY_BUFFER_SIZE = 2 ** 14


def _iter_file(src_file, close=False):
    try:
        while True:
            block = src_file.read()
            if not block:
                break
            yield block
    finally:
        if close:
            src_file.close()


class TableRow(dict):
    """ Database row, represented as a Python `dict`.

    .. attribute:: id

        Primary key of this row.
    """

    id = None

    def delete(self):
        """ Execute a `DELETE` query for this row. """
        self._parent_table.delete(self.id, _deprecation_warning=False)

    def save(self):
        """ Execute an `UPDATE` query for this row. """
        self._parent_table.save(self, _deprecation_warning=False)


class DbFile(object):
    """ Database binary blob. It works like a file, but has a simpler API,
    with methods to read and write a stream of byte chunks.

    .. attribute:: id

        Unique ID of the file. It can be used to request the file later
        via :meth:`Session.get_db_file`.
    """

    def __init__(self, session, id):
        self.id = id
        self._session = session

    def save_from(self, in_file):
        """ Consume data from `in_file` (a file-like object) and save to
        database. """
        lobject = self._session.conn.lobject(self.id, 'wb')
        try:
            for block in _iter_file(in_file):
                lobject.write(block)
        finally:
            lobject.close()

    def iter_data(self):
        """ Read data from database and return it as a Python generator. """
        lobject = self._session.conn.lobject(self.id, 'rb')
        return _iter_file(lobject, close=True)


class PostgresqlDB(object):
    """ A pool of reusable database connections that get created on demand. """

    def __init__(self, connection_uri, schema=None, debug=False):
        global psycopg2
        import psycopg2.pool
        import psycopg2.extras
        if schema is None:
            schema = Schema([])
        self._schema = schema
        params = transform_connection_uri(connection_uri)
        self._conn_pool = psycopg2.pool.ThreadedConnectionPool(0, 5, **params)
        self._debug = debug

    def _get_connection(self):
        conn = self._conn_pool.getconn()
        psycopg2.extras.register_hstore(conn, globally=False, unicode=True)
        return conn

    def get_session(self, lazy=False):
        """ Get a :class:`Session` for talking to the database. If `lazy` is
        True then the connection is estabilished only when the first query
        needs to be executed. """
        if lazy:
            conn = _lazy
        else:
            conn = self._get_connection()
        session = Session(self._schema, conn)
        session._pool = self
        if self._debug:
            session._debug = True
        return session

    def put_session(self, session):
        """ Retire the session, freeing up its connection, and aborting any
        non-committed transaction. """
        if session._conn is not _lazy:
            self._conn_pool.putconn(session._release_conn())

    @contextmanager
    def session(self):
        s = self.get_session()
        try:
            yield s
        finally:
            self.put_session(s)


class Schema(object):

    def __init__(self, names=[]):
        self._by_name = {}
        for name in names:
            self.define_table(name, name)

    def define_table(self, cls_name, table_name):
        # TODO make sure table_name is safe

        class cls(TableRow):
            _table = table_name
        cls.__name__ = cls_name

        self._by_name[table_name] = cls
        return cls

    def __getitem__(self, name):
        return self._by_name[name]

    def __iter__(self):
        return iter(self._by_name)

    def bind(self, connection_uri, debug=False):
        msg = "Schema.bind() is deprecated; use PostgresqlDB() instead."
        warnings.warn(msg, DeprecationWarning, stacklevel=2)
        return PostgresqlDB(connection_uri, self, debug)


class PostgresqlDialect(object):

    _missing_table_pattern = re.compile(r'^relation "([^"]+)" does not exist')

    def __init__(self, conn):
        self.conn = conn

    def execute(self, *args, **kwargs):
        cursor = kwargs.get('cursor') or self.conn.cursor()
        try:
            cursor.execute(*args)
        except Exception, e:
            from psycopg2 import ProgrammingError
            if isinstance(e, ProgrammingError):
                if e.args:
                    m = self._missing_table_pattern.match(e.args[0])
                    if m is not None:
                        name = m.group(1)
                        raise MissingTable(name)
            raise
        return cursor

    def create_table(self, name):
        self.execute("CREATE TABLE IF NOT EXISTS " + name + " ("
                     "id SERIAL PRIMARY KEY, "
                     "data HSTORE)")

    def drop_table(self, name):
        self.execute("DROP TABLE IF EXISTS " + name)

    def insert(self, name, obj):
        cursor = self.execute("INSERT INTO " + name +
                              " (data) VALUES (%s)",
                              (obj,))
        self.execute("SELECT CURRVAL(%s)", (name + '_id_seq',),
                     cursor=cursor)
        [(last_insert_id,)] = list(cursor)
        return last_insert_id

    def select_by_id(self, name, obj_id):
        cursor = self.execute("SELECT data FROM " + name +
                              " WHERE id = %s",
                              (obj_id,))
        return list(cursor)

    def select_all(self, name):
        return self.execute("SELECT id, data FROM " + name)

    def update(self, name, obj_id, obj):
        self.execute("UPDATE " + name + " SET data = %s WHERE id = %s",
                     (obj, obj_id))

    def delete(self, name, obj_id):
        self.execute("DELETE FROM " + name + " WHERE id = %s", (obj_id,))


class SqliteDialect(object):

    _missing_table_pattern = re.compile(r'^no such table: (.+)')

    def __init__(self, conn):
        self.conn = conn

    def execute(self, *args):
        cursor = self.conn.cursor()
        try:
            cursor.execute(*args)
        except Exception, e:
            import sqlite3
            if isinstance(e, sqlite3.OperationalError):
                if e.args:
                    m = self._missing_table_pattern.match(e.args[0])
                    if m is not None:
                        name = m.group(1)
                        raise MissingTable(name)
            raise
        return cursor

    def create_table(self, name):
        self.execute("CREATE TABLE IF NOT EXISTS " + name + " ("
                     "id INTEGER PRIMARY KEY, "
                     "data BLOB)")

    def drop_table(self, name):
        self.execute("DROP TABLE IF EXISTS " + name)

    def select_by_id(self, name, obj_id):
        cursor = self.execute("SELECT data FROM " + name +
                               " WHERE id = ?",
                               (obj_id,))
        return [(json.loads(r[0]),) for r in cursor]

    def select_all(self, name):
        cursor = self.execute("SELECT id, data FROM " + name)
        return ((id, json.loads(data)) for (id, data) in cursor)

    def insert(self, name, obj):
        cursor = self.execute("INSERT INTO " + name +
                              " (data) VALUES (?)",
                              (json.dumps(obj),))
        return cursor.lastrowid

    def update(self, name, obj_id, obj):
        self.execute("UPDATE " + name + " SET data = ? WHERE id = ?",
                     (json.dumps(obj), obj_id))

    def delete(self, name, obj_id):
        self.execute("DELETE FROM " + name + " WHERE id = ?", (obj_id,))


class Table(object):
    """ A database table with two columns: ``id`` (integer primary key) and
    ``data`` (hstore). """

    RowNotFound = RowNotFound

    MultipleRowsFound = MultipleRowsFound

    def __init__(self, row_cls, session):
        self._session = session
        self._row_cls = row_cls
        self._name = row_cls._table

    @property
    def sql(self):
        return self._session.sql

    def create_table(self):
        return self.sql.create_table(self._name)

    def drop_table(self):
        return self.sql.drop_table(self._name)

    def _row(self, id=None, data={}):
        ob = self._row_cls(data)
        ob.id = id
        ob._parent_table = self
        return ob

    def new(self, *args, **kwargs):
        """ Create a new :class:`TableRow` with auto-incremented `id`. An
        `INSERT` SQL query is executed to generate the id. """

        row = self._row(data=dict(*args, **kwargs))
        row.save()
        return row

    def save(self, obj, _deprecation_warning=True):
        if _deprecation_warning:
            msg = "Table.save(row) is deprecated; use row.save() instead."
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
        if self._session._debug:
            for key, value in obj.iteritems():
                assert isinstance(key, basestring), \
                    "Key %r is not a string" % key
                assert isinstance(value, basestring), \
                    "Value %r for key %r is not a string" % (value, key)
        if obj.id is None:
            obj.id = self.sql.insert(self._name, obj)
        else:
            self.sql.update(self._name, obj.id, obj)

    def get(self, obj_id):
        """ Fetches the :class:`TableRow` with the given `id`. """

        rows = self.sql.select_by_id(self._name, obj_id)
        if len(rows) == 0:
            raise RowNotFound("No %r with id=%d" % (self._row_cls, obj_id))
        [(data,)] = rows
        return self._row(obj_id, data)

    def delete(self, obj_id, _deprecation_warning=True):
        if _deprecation_warning:
            msg = "Table.delete(row) is deprecated; use row.delete() instead."
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
        assert isinstance(obj_id, (int, long))
        self.sql.delete(self._name, obj_id)

    def get_all(self, _deprecation_warning=True):
        if _deprecation_warning:
            msg = "Table.get_all() is deprecated; use Table.find() instead."
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
        return self.find()

    def find(self, **kwargs):
        """ Returns an iterator over all matching :class:`TableRow`
        objects. """

        for ob_id, ob_data in self.sql.select_all(self._name):
            row = self._row(ob_id, ob_data)
            if all(row.get(k) == kwargs[k] for k in kwargs):
                yield row

    def find_first(self, **kwargs):
        """ Shorthand for calling :meth:`find` and getting the first result.
        Raises `RowNotFound` if no result is found. """

        for row in self.find(**kwargs):
            return row
        else:
            raise RowNotFound

    def find_single(self, **kwargs):
        """ Shorthand for calling :meth:`find` and getting the first result.
        Raises `RowNotFound` if no result is found. Raises `MultipleRowsFound`
        if more than one result is found. """

        results = iter(self.find(**kwargs))

        try:
            row = results.next()
        except StopIteration:
            raise RowNotFound

        try:
            results.next()
        except StopIteration:
            pass
        else:
            raise MultipleRowsFound("More than one row found")

        return row


_expired = object()
_lazy = object()


class Session(object):
    """ Wrapper for a database connection with methods to access tables and
    commit/rollback transactions. """

    _debug = False
    _dialect_cls = PostgresqlDialect

    def __init__(self, schema, conn, debug=False):
        self._schema = schema
        self._conn = conn

    @property
    def conn(self):
        if self._conn is _expired:
            raise RuntimeError("Error: trying to use expired database session")
        elif self._conn is _lazy:
            self._conn = self._pool._get_connection()
        return self._conn

    @property
    def sql(self):
        return self._dialect_cls(self.conn)

    def _release_conn(self):
        conn = self._conn
        self._conn = _expired
        return conn

    def get_db_file(self, id=None):
        """ Access a :class:`DbFile`. If `id` is `None`, a new file is created;
        otherwise, the requested blob file is returned by id. """
        if id is None:
            id = self.conn.lobject(mode='n').oid
        return DbFile(self, id)

    def del_db_file(self, id):
        """ Delete the :class:`DbFile` object with the given `id`. """
        self.conn.lobject(id, mode='n').unlink()

    def commit(self):
        """ Commit the current transaction. """
        self.conn.commit()

    def rollback(self):
        """ Roll back the current transaction. """
        # TODO needs a unit test
        self.conn.rollback()

    def _table_for_cls(self, obj_or_cls):
        if isinstance(obj_or_cls, TableRow):
            row_cls = type(obj_or_cls)
        elif issubclass(obj_or_cls, TableRow):
            row_cls = obj_or_cls
        else:
            raise ValueError("Can't determine table type from %r" %
                             (obj_or_cls,))
        return Table(row_cls, self)

    def table(self, obj_or_cls):
        msg = ("Session.table(RowCls) is deprecated; use "
               "session['table_name'] instead.")
        warnings.warn(msg, DeprecationWarning, stacklevel=2)
        return self._table_for_cls(obj_or_cls)

    def _tables(self):
        for name in self._schema:
            yield self[name]

    def __getitem__(self, name):
        """ Get the :class:`Table` called `name`. """
        try:
            row_cls = self._schema[name]
        except KeyError:
            class row_cls(TableRow):
                _table = name
        return self._table_for_cls(row_cls)

    def save(self, obj, _deprecation_warning=True):
        if _deprecation_warning:
            msg = "Session.save(row) is deprecated; use row.save() instead."
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
        self._table_for_cls(obj).save(obj, _deprecation_warning=False)

    def create_all(self):
        """ Make sure all tables defined by the schema exist in the
        database. """
        for table in self._tables():
            table.create_table()
        self._conn.commit()

    def delete_all_blobs(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT oid FROM pg_largeobject_metadata")
        for [oid] in cursor:
            self.conn.lobject(oid, 'n').unlink()

    def drop_all(self):
        """ Drop all tables defined by the schema and delete all blob
        files. """
        for table in self._tables():
            table.drop_table()
        self.delete_all_blobs()
        self._conn.commit()


class SqliteDbFile(object):

    def __init__(self, session, id, data):
        self.id = id
        self._data = data

    def save_from(self, in_file):
        self._data.seek(0)
        self._data.write(in_file.read())

    def iter_data(self):
        return iter([self._data.getvalue()])


class SqliteSession(Session):

    _dialect_cls = SqliteDialect

    def __init__(self, schema, conn, db_files, debug=False):
        super(SqliteSession, self).__init__(schema, conn, debug)
        self._db_files = db_files

    def get_db_file(self, id=None):
        if self._db_files is None:
            raise BlobsNotSupported
        if id is None:
            while True:
                id = random.randint(1, 10 ** 6)
                if id not in self._db_files:
                    break
            self._db_files[id] = StringIO.StringIO()
        return SqliteDbFile(self, id, self._db_files[id])

    def del_db_file(self, id):
        del self._db_files[id]

    def delete_all_blobs(self):
        pass

    def drop_all(self):
        for table in self._tables():
            table.drop_table()
        self._conn.commit()
        self._db_files.clear()


class SqliteDB(object):
    """ SQLite database session pool; same api as :class:`PostgresqlDB` """

    def __init__(self, uri, schema=None):
        import sqlite3
        self._connect = lambda: sqlite3.connect(uri)
        if uri == ':memory:':
            _single_connection = self._connect()
            self._connect = lambda: _single_connection
            self.put_session = lambda session: None
            self._files = {}
        else:
            self._files = None
        if schema is None:
            schema = Schema([])
        self.schema = schema

    def get_session(self):
        return SqliteSession(self.schema, self._connect(), self._files)

    def put_session(self, session):
        session._release_conn().close()

    @contextmanager
    def session(self):
        s = self.get_session()
        try:
            yield s
        finally:
            self.put_session(s)


def transform_connection_uri(connection_uri):
    m = re.match(r"^postgresql://"
                 r"((?P<user>[^:]*)(:(?P<password>[^@]*))@?)?"
                 r"(?P<host>[^/]+)/(?P<db>[^/]+)$",
                 connection_uri)
    if m is None:
        raise ValueError("Can't parse connection URI %r" % connection_uri)
    return {
        'database': m.group('db'),
        'host': m.group('host'),
        'user': m.group('user'),
        'password': m.group('password'),
    }
