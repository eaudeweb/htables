import re
import json
import psycopg2.pool, psycopg2.extras


COPY_BUFFER_SIZE = 2**14
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

    id = None


class DbFile(object):

    def __init__(self, session, id):
        self.id = id
        self._session = session

    def save_from(self, in_file):
        lobject = self._session.conn.lobject(self.id, 'wb')
        try:
            for block in _iter_file(in_file):
                lobject.write(block)
        finally:
            lobject.close()

    def iter_data(self):
        lobject = self._session.conn.lobject(self.id, 'rb')
        return _iter_file(lobject, close=True)


class SessionPool(object):

    def __init__(self, schema, connection_uri, debug):
        self._schema = schema
        params = transform_connection_uri(connection_uri)
        self._conn_pool = psycopg2.pool.ThreadedConnectionPool(0, 5, **params)
        self._debug = debug

    def get_session(self):
        conn = self._conn_pool.getconn()
        psycopg2.extras.register_hstore(conn, globally=False, unicode=True)
        session = Session(self._schema, conn)
        if self._debug:
            session._debug = True
        return session

    def put_session(self, session):
        self._conn_pool.putconn(session._release_conn())


class Schema(object):

    def __init__(self):
        self.tables = []

    def define_table(self, cls_name, table_name):
        # TODO make sure table_name is safe

        class cls(TableRow):
            _table = table_name
        cls.__name__ = cls_name

        self.tables.append(cls)
        return cls

    def bind(self, connection_uri, debug=False):
        return SessionPool(self, connection_uri, debug)


class Table(object):

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
        cursor.execute("DELETE FROM " + self._name + " WHERE id = %s", (obj_id,))

    def save(self, obj):
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
        rows = self._select_by_id(obj_id)
        if len(rows) == 0:
            raise KeyError("No %r with id=%d" % (self._row_cls, obj_id))
        [(data,)] = rows
        obj = self._row_cls(data)
        obj.id = obj_id
        return obj

    def delete(self, obj_id):
        assert isinstance(obj_id, int)
        self._delete(obj_id)

    def get_all(self):
        for ob_id, ob_data in self._select_all():
            ob = self._row_cls(ob_data)
            ob.id = ob_id
            yield ob


class Session(object):

    _debug = False
    _table_cls = Table

    def __init__(self, schema, conn, debug=False):
        self._schema = schema
        self._conn = conn

    @property
    def conn(self):
        if self._conn is None:
            raise ValueError("Error: trying to use expired database session")
        return self._conn

    def _release_conn(self):
        conn = self._conn
        self._conn = None
        return conn

    def get_db_file(self, id=None):
        if id is None:
            id = self.conn.lobject(mode='n').oid
        return DbFile(self, id)

    def del_db_file(self, id):
        self.conn.lobject(id, mode='n').unlink()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        # TODO needs a unit test
        self.conn.rollback()

    def table(self, obj_or_cls):
        if isinstance(obj_or_cls, TableRow):
            row_cls = type(obj_or_cls)
        elif issubclass(obj_or_cls, TableRow):
            row_cls = obj_or_cls
        else:
            raise ValueError("Can't determine table type from %r" %
                             (obj_or_cls,))
        return self._table_cls(row_cls, self)

    def save(self, obj):
        self.table(obj).save(obj)

    def create_all(self):
        for row_cls in self._schema.tables:
            self.table(row_cls)._create()
        self._conn.commit()

    def drop_all(self):
        for row_cls in self._schema.tables:
            self.table(row_cls)._drop()
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
        cursor.execute("DELETE FROM " + self._name + " WHERE id = ?", (obj_id,))


class SqliteSession(Session):

    _table_cls = SqliteTable

    def __init__(self, schema, conn, db_files, debug=False):
        super(SqliteSession, self).__init__(schema, conn, debug)
        self._db_files = db_files

    def get_db_file(self, id=None):
        if id is None:
            import random, string, StringIO
            while True:
                id = ''.join(random.choice(string.ascii_letters)
                             for c in range(6))
                if id not in self._db_files:
                    break
            self._db_files[id] = StringIO.StringIO()
        return SqliteDbFile(self, id, self._db_files[id])

    def del_db_file(self, id):
        del self._db_files[id]


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
