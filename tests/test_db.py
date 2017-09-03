import pymysql
import warnings
warnings.filterwarnings("ignore")

from StreamingSQL.db import create_connection, execute_command
from StreamingSQL.fonts import Colors, Formats

host = '10.0.0.122'
port = 3306
usr = 'root'
paswd = ''
db = 'test'


def test_default_create_connection():
    """
    Test the connection to database
    Assert:
        Connection occurs
    """
    cur = create_connection(host=host, port=port, user=usr, password=paswd, db=db)
    assert type(cur) == pymysql.cursors.Cursor


# def test_fail_create_connection():
#     """
#     Test that the connection fails
#     Assert:
#         Failure to connection occurs
#     """
#     # Error message
#     error = "2003: Cant connect to MySQL server on '%s' [Errno 61] Connection refused"
#     error = (Formats.BOLD+Colors.RED+"Connection Error - "+error+Formats.END+Colors.END) % host[:-1]
#
#     # Host failure
#     cur = create_connection(host=host[:-1], port=port, user=usr, password=paswd, db=db)
#     try:
#         assert cur == error
#     except AssertionError:
#         pass
#
#     error = (Formats.BOLD + Colors.RED + "Connection Error - " + error + Formats.END + Colors.END) % host
#     # Port fails
#     cur = create_connection(host=host, port=port+10, user=usr, password=paswd, db=db)
#     try:
#         assert cur == error
#     except AssertionError:
#         pass
#
#     # User fails
#     cur = create_connection(host=host, port=port, user=usr[:-1], password=paswd, db=db)
#     try:
#         assert cur == error
#     except AssertionError:
#         pass


# def test_execute_command():
#     cur = create_connection(host=host, port=port, user=usr, password=paswd, db=db)
#     assert type(cur) == pymysql.cursors.Cursor
#     stmt = "SELECT `SCHEMA_NAME` from `INFORMATION_SCHEMA`.`SCHEMATA` WHERE `SCHEMA_NAME` LIKE '%s';" % db
#     result = execute_command(cur, stmt)
#     assert result[0][0] == db
#
#
# def test_new_db_create_connection():
#     db="db2"
#     cur = create_connection(host=host, port=port, user=usr, password=paswd, db=db)
#     assert type(cur) == pymysql.cursors.Cursor
#     stmt = "SELECT `SCHEMA_NAME` from `INFORMATION_SCHEMA`.`SCHEMATA` WHERE `SCHEMA_NAME` LIKE '%s';" % db
#     result = execute_command(cur, stmt)
#     assert result[0][0] == db
#
