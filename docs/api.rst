===
API
===

.. module:: htables


Session pool
------------
.. autoclass:: htables.SessionPool
  :members:

Session
-------
.. autoclass:: htables.Session
  :members: __getitem__, get_db_file, del_db_file, commit, rollback,
            create_all, drop_all

Table
-----
.. autoclass:: htables.Table
  :members:

Row and file classes
--------------------
.. autoclass:: htables.TableRow
  :members:

.. autoclass:: htables.DbFile
  :members:
