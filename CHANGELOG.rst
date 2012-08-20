0.4 (2012-08-20)
----------------
* Bring SQLite backend API on par with PostgreSQL.
* Refactor Table code; extract backend-specific SQL wrappers.
* Explicit schema definition no longer required.
* Catch common SQL errors and re-raise custom exceptions.

0.3 (2012-06-24)
----------------
* Lazy session that fetches a connection when first needed.

0.3-rc1 (2012-06-23)
--------------------
* Several API changes to make it more pythonic.
* New `find`, `find_first`, `find_single` methods to fetch rows.
* Support for Python 2.5

0.2.1 (2012-05-14)
------------------
* Bugfix - SQLite backend should use integer IDs.

0.2 (2012-05-04)
----------------
* SQLite in-memory backend.

0.1 (2012-05-02)
----------------
* Initial release with PostgreSQL support, including large files.
