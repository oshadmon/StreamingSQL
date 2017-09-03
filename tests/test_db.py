"""
The following tests that db connections works properly.
Make sure the default configurations match your connection to the database
"""
import pymysql
import warnings
warnings.filterwarnings("ignore")

from StreamingSQL.db import create_connection, execute_command
from StreamingSQL.fonts import Colors, Formats

"""Default configuration to connect to the DB"""
host = 'localhost'
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


def test_wrong_host_fail_create_connection():
    """
        Test that error is properly returned when there is an incorrect host 
        Assert:
            Proper error is returned/formatted
    """
    error = "2003: Cant connect to MySQL server on '%s' [Errno 61] Connection refused"
    error = (Formats.BOLD + Colors.RED + "Connection Error - " + error + Formats.END + Colors.END) % host[:-3]


    cur = create_connection(host=host[:-3], port=port, user=usr, password=paswd, db=db)
    try:
        assert cur == error
    except AssertionError:
        pass


def test_wrong_port_fail_create_connection():
    """
            Test that error is properly returned when there is an incorrect port number 
            Assert:
                Proper error is returned/formatted
    """
    error = "2003: Cant connect to MySQL server on '%s' [Errno 61] Connection refused"
    error = (Formats.BOLD + Colors.RED + "Connection Error - " + error + Formats.END + Colors.END) % host

    cur = create_connection(host=host, port=port + 13, user=usr, password=paswd, db=db)
    try:
        assert cur == error
    except AssertionError:
        pass


def test_wrong_user_fail_create_connection():
    """
        Test that error is properly returned when there is an incorrect user 
        Assert:
            Proper error is returned/formatted
    """
    error = "2003: Cant connect to MySQL server on '%s' [Errno 61] Connection refused"
    error = (Formats.BOLD + Colors.RED + "Connection Error - " + error + Formats.END + Colors.END) % host

    cur = create_connection(host=host, port=port, user='', password=paswd, db=db)
    try:
        assert cur == error
    except AssertionError:
        pass


def test_wrong_passwd_fail_create_connection():
    """
    Test that error is properly returned when there is an incorrect password 
    Assert:
        Proper error is returned/formatted
    """
    error = "2003: Cant connect to MySQL server on '%s' [Errno 61] Connection refused"
    error = (Formats.BOLD + Colors.RED + "Connection Error - " + error + Formats.END + Colors.END) % host

    cur = create_connection(host=host, port=port, user=usr, password=usr, db=db)
    try:
        assert cur == error
    except AssertionError:
        pass


def test_execute_command():
    """
    Execute "SELECT 1;"  
    Assert:
        A result of 1 is returned
    """
    cur = create_connection(host=host, port=port, user=usr, password=paswd, db=db)
    assert type(cur) == pymysql.cursors.Cursor
    stmt = "SELECT 1"
    result = execute_command(cur, stmt)
    assert result[0][0] == 1


def test_syntax_fail_execute_command():
    """
    Execute "SLCT 1;"
    Assert:
        An error message is returned
    """
    stmt = "SLCT 1"
    error = ("1064: You have an error in your SQL syntax; check the manual that corresponds to your MariaDB server "+
             "version for the right syntax to use near '%s' at line 1")
    error = Formats.BOLD + Colors.RED + "Connection Error - " + error % stmt + Formats.END + Colors.END

    cur = create_connection(host=host, port=port, user=usr, password=paswd, db=db)
    assert type(cur) == pymysql.cursors.Cursor
    result = execute_command(cur, stmt)
    try:
        assert result == error
    except AssertionError:
        pass


def test_new_db_create_connection():
    """
    Create a connection to a new database 
    Assert:
        New database is created/removed
    """
    db="db2"
    cur = create_connection(host=host, port=port, user=usr, password=paswd, db=db)
    assert type(cur) == pymysql.cursors.Cursor
    stmt = "SELECT `SCHEMA_NAME` from `INFORMATION_SCHEMA`.`SCHEMATA` WHERE `SCHEMA_NAME` LIKE '%s';" % db
    result = execute_command(cur, stmt)
    assert result[0][0] == db

    stmt = "FLUSH TABLES; DROP DATABASE IF EXISTS %s;" % db
    result = execute_command(cur, stmt)
    assert result == ()


