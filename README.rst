.. role:: class


HTables
=======

HTables is a database library for storing mapping objects in a relational
database. Two backends are supported so far: :class:`~htables.PostgresqlDB`
(using the hstore_ extension; requires psycopg2_) and
:class:`~htables.SQLiteDB`.

.. _hstore: http://www.postgresql.org/docs/current/static/hstore.html
.. _psycopg2: http://initd.org/psycopg/

::

    >>> import htables
    >>> db = htables.SqliteDB(':memory:')
    >>> session = db.get_session()
    >>> session['tweet'].create_table()
    >>> session.commit()

Tables are collections of Rows. A row is basically a dictionary with an
extra ``id`` property. Its keys and values must be strings.

::

    >>> tweet_table = session['tweet']
    >>> tweet = tweet_table.new(message="Hello world!")
    >>> tweet['author'] = '1337 h4x0r'
    >>> tweet.save()
    >>> session.commit()

There are many ways of retrieving rows. The following all fetch the
same record::

    >>> [tweet] = list(tweet_table.find())
    >>> [tweet] = list(tweet_table.find(author='1337 h4x0r'))
    >>> tweet = tweet_table.find_first()
    >>> tweet = tweet_table.find_single()
    >>> tweet = tweet_table.get(1)


Links
-----

* documentation_
* `source code`_

.. _documentation: http://eaudeweb.github.com/htables/
.. _source code: https://github.com/eaudeweb/htables/
