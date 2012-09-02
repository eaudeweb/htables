.. role:: class


HTables
=======

HTables is a library for storing string-to-string mapping objects in a
database. Two backends are supported so far:
:class:`~htables.PostgresqlDB` and :class:`~htables.SqliteDB`.

.. _hstore: http://www.postgresql.org/docs/current/static/hstore.html
.. _psycopg2: http://initd.org/psycopg/

::

    >>> import htables
    >>> db = htables.SqliteDB(':memory:')
    >>> with db.session() as dbs:
    ...     dbs['tweet'].create_table()
    ...     dbs['tweet'].new(text="Hello world!")
    ...     dbs.commit()

Tables are collections of Rows. A row is basically a dictionary with an
extra ``id`` property. Its keys and values must be strings.

::

    >>> with db.session() as dbs:
    ...     tweet = dbs['tweet'].find_first()
    ...     tweet['author'] = '1337 h4x0r'
    ...     tweet.save()
    ...     dbs.commit()

There are many ways of retrieving rows. The following all fetch the
same record::

    >>> with db.session() as dbs:
    ...     tweet_table = dbs['tweet']
    ...     [tweet] = list(tweet_table.find())
    ...     [tweet] = list(tweet_table.find(author='1337 h4x0r'))
    ...     tweet = tweet_table.find_first()
    ...     tweet = tweet_table.find_single()
    ...     tweet = tweet_table.get(1)


Links
-----

* documentation_
* `source code`_

.. _documentation: http://packages.python.org/htables/
.. _source code: https://github.com/eaudeweb/htables/
