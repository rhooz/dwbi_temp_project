import pymysql
import pymssql
import psycopg2
import cx_Oracle

class Connection(object):
    """
    factory class for db connections
    """

    def factory(config, type):
        """
        connect to the applicable database by type
        send in i2ap configuration and database type, the appropriate connection object will be instantiated/returned

        usage example: mycon = Connection.factory(myConfig, 'postgres')

        :param config: i2ap configuration object
        :param type: the database type
        :return: connection
        """
        # MySQL uses pymysql
        if type == "mysql":
            return(pymysql.connect(host=config['database-server'], user=config['database-user'], password=config['database-password'], charset='utf8mb4'))
        # SQL Server uses pymssql
        elif type=="mssql":
            return(pymssql.connect(server=config['database-server'], user=config['database-user'], password=config['database-password'], database=config['database'], charset='utf8mb4'))
        # Postgres uses psycopg2
        elif type == "postgres":
            return(psycopg2.connect(host=config['database-server'], port=config['database-port'], user=config['database-user'], password=config['database-password'], database=config['database']))
        # Oracle uses cx_Oracle
        elif type == "oracle":
            dsn_tns = cx_Oracle.makedsn(config['database-server'], config['database-port'], config['database-sid'])
            return (cx_Oracle.connect(user=config['database-user'],password=config['database-password'], dsn=dsn_tns))
        else:
            raise TypeError('Invalid database type')
    factory = staticmethod(factory)
