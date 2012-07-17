import re
try:
    import simplejson as json
except ImportError:
    import json
import random
import StringIO
import warnings
import psycopg2.pool
import psycopg2.extras


class BlobsNotSupported(Exception):
    """ This database does not support blobs. """


class RowNotFound(KeyError):
    """ No row matching search criteria. """
    # TODO don't subclass from KeyError


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

    def __init__(self, connection_uri, schema, debug=False):
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


class Table(object):
    """ A database table with two columns: ``id`` (integer primary key) and
    ``data`` (hstore). """

    def __init__(self, row_cls, session):
        self._session = session
        self._row_cls = row_cls
        self._name = row_cls._table

    def _create(self):
        cursor = self._session.conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS " + self._name + " ("
                            "id SERIAL PRIMARY KEY, "
                            "data HSTORE)")

    def _drop(self):
        cursor = self._session.conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS " + self._name)

    def _insert(self, obj):
        cursor = self._session.conn.cursor()
        cursor.execute("INSERT INTO " + self._name + " (data) VALUES (%s)",
                       (obj,))
        cursor.execute("SELECT CURRVAL(%s)", (self._name + '_id_seq',))
        [(last_insert_id,)] = list(cursor)
        return last_insert_id

    def _select_by_id(self, obj_id):
        cursor = self._session.conn.cursor()
        cursor.execute("SELECT data FROM " + self._name + " WHERE id = %s",
                       (obj_id,))
        return list(cursor)

    def _select_all(self):
        cursor = self._session.conn.cursor()
        cursor.execute("SELECT id, data FROM " + self._name)
        return cursor

    def _update(self, obj_id, obj):
        cursor = self._session.conn.cursor()
        cursor.execute("UPDATE " + self._name + " SET data = %s WHERE id = %s",
                       (obj, obj_id))

    def _delete(self, obj_id):
        cursor = self._session.conn.cursor()
        cursor.execute("DELETE FROM " + self._name + " WHERE id = %s",
                       (obj_id,))

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
            obj.id = self._insert(obj)
        else:
            self._update(obj.id, obj)

    def get(self, obj_id):
        """ Fetches the :class:`TableRow` with the given `id`. """

        rows = self._select_by_id(obj_id)
        if len(rows) == 0:
            raise RowNotFound("No %r with id=%d" % (self._row_cls, obj_id))
        [(data,)] = rows
        return self._row(obj_id, data)

    def delete(self, obj_id, _deprecation_warning=True):
        if _deprecation_warning:
            msg = "Table.delete(row) is deprecated; use row.delete() instead."
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
        assert isinstance(obj_id, (int, long))
        self._delete(obj_id)

    def get_all(self, _deprecation_warning=True):
        if _deprecation_warning:
            msg = "Table.get_all() is deprecated; use Table.find() instead."
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
        return self.find()

    def find(self, **kwargs):
        """ Returns an iterator over all matching :class:`TableRow`
        objects. """

        for ob_id, ob_data in self._select_all():
            row = self._row(ob_id, ob_data)
            if all(row[k] == kwargs[k] for k in kwargs):
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
        Raises `RowNotFound` if no result is found. Raises `ValueError` if more
        than one result is found. """

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
            raise ValueError("More than one row found")

        return row


_expired = object()
_lazy = object()


class Session(object):
    """ Wrapper for a database connection with methods to access tables and
    commit/rollback transactions. """

    _debug = False
    _table_cls = Table

    def __init__(self, schema, conn, debug=False):
        self._schema = schema
        self._conn = conn

    @property
    def conn(self):
        if self._conn is _expired:
            raise ValueError("Error: trying to use expired database session")
        elif self._conn is _lazy:
            self._conn = self._pool._get_connection()
        return self._conn

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
        return self._table_cls(row_cls, self)

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
        return self._table_for_cls(self._schema[name])

    def save(self, obj, _deprecation_warning=True):
        if _deprecation_warning:
            msg = "Session.save(row) is deprecated; use row.save() instead."
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
        self._table_for_cls(obj).save(obj, _deprecation_warning=False)

    def create_all(self):
        """ Make sure all tables defined by the schema exist in the
        database. """
        for table in self._tables():
            table._create()
        self._conn.commit()

    def drop_all(self):
        """ Drop all tables defined by the schema and delete all blob
        files. """
        for table in self._tables():
            table._drop()
        cursor = self.conn.cursor()
        cursor.execute("SELECT oid FROM pg_largeobject_metadata")
        for [oid] in cursor:
            self.conn.lobject(oid, 'n').unlink()
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


class SqliteTable(Table):

    def _create(self):
        cursor = self._session.conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS " + self._name + " ("
                            "id INTEGER PRIMARY KEY, "
                            "data BLOB)")

    def _insert(self, obj):
        cursor = self._session.conn.cursor()
        cursor.execute("INSERT INTO " + self._name + " (data) VALUES (?)",
                       (json.dumps(obj),))
        return cursor.lastrowid

    def _select_by_id(self, obj_id):
        cursor = self._session.conn.cursor()
        cursor.execute("SELECT data FROM " + self._name + " WHERE id = ?",
                       (obj_id,))
        return [(json.loads(r[0]),) for r in cursor]

    def _select_all(self):
        cursor = self._session.conn.cursor()
        cursor.execute("SELECT id, data FROM " + self._name)
        return ((id, json.loads(data)) for (id, data) in cursor)

    def _update(self, obj_id, obj):
        cursor = self._session.conn.cursor()
        cursor.execute("UPDATE " + self._name + " SET data = ? WHERE id = ?",
                       (json.dumps(obj), obj_id))

    def _delete(self, obj_id):
        cursor = self._session.conn.cursor()
        cursor.execute("DELETE FROM " + self._name + " WHERE id = ?",
                       (obj_id,))


class SqliteSession(Session):

    _table_cls = SqliteTable

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

    def drop_all(self):
        for table in self._tables():
            table._drop()
        self._conn.commit()
        self._db_files.clear()


class SqliteDB(object):

    def __init__(self, uri, schema):
        import sqlite3
        self._connect = lambda: sqlite3.connect(uri)
        if uri == ':memory:':
            _single_connection = self._connect()
            self._connect = lambda: _single_connection
            self.put_session = lambda session: None
            self._files = {}
        else:
            self._files = None
        self.schema = schema

    def get_session(self):
        return SqliteSession(self.schema, self._connect(), self._files)

    def put_session(self, session):
        session._release_conn().close()


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
