import pymysql
import warnings
warnings.filterwarnings("ignore")

from StreamingSQL.fonts import Colors, Formats

def create_connection(host='localhost', port=3306, user='root', password='', db='test')->pymysql.cursors.Cursor:
    """
    Create a connection to the MySQL node
    Args:
        host: MySQL connection host
        port: MySQL connection port
        user: user connecting to MySQL
        password: user password
        db: database name 
    Returns:
        A connection to the MySQL that can be executed against
    """
    conn = None
    try:
        conn = pymysql.connect(host=host, port=port, user=user, passwd=password, db='test')
    except pymysql.err.OperationalError as e:
        error = str(e).replace("(",")").replace('"','').replace(")","").replace(",",":")
        print(Formats.BOLD+Colors.RED+"Connection Error - "+error+Formats.END+Colors.END)
        return Formats.BOLD+Colors.RED+"Connection Error - "+error+Formats.END+Colors.END

    if db is not 'test':
        cur = conn.cursor()
        output = execute_command(cur, "CREATE DATABASE IF NOT EXISTS %s;" % db)
        if output == 1:
            return output
        try:
            conn = pymysql.connect(host=host, port=port, user=user, passwd=password, db=db)
        except pymysql.err.OperationalError as e:
            print(Formats.BOLD + Colors.RED + "Connection Error: " + str(e) + Formats.END + Colors.END)

    try:
        return conn.cursor()
    except AttributeError:
        return 1


def execute_command(cur=None, stmt="")->tuple:
    """
    Execute SQL command
    Args:
        cur: connection 
        stmt: SQL stmt
    Returns:
        (by default) result of the sql execution 
    """
    cur.execute(stmt)
    return cur.fetchall()

if __name__ == '__main__':
    cur = create_connection()