# Import necessary modules
from psycopg_pool import ConnectionPool
import psycopg_pool

class DB:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DB, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        if not hasattr(self, '_pool'):
            # Update these values with the actual host, port, username, password, and database name
            host = '10.63.20.87'
            port = 9573
            user = 'postgres'
            password = 'tcs@123'
            dbname = 'green-coding'

            # Construct the connection string
            self._pool = ConnectionPool(
                f"postgresql://{user}:{password}@{host}:{port}/{dbname}",
                min_size=1,
                max_size=2,
                open=True
            )

    def __query(self, query, params=None, return_type=None, row_factory=None):
        ret = False

        try:
            with self._pool.connection() as conn:
                conn.autocommit = False # should be default, but we are explicit
                cur = conn.cursor(row_factory=row_factory) # None is actually the default cursor factory
                if isinstance(query, list) and isinstance(params, list) and len(query) == len(params):
                    for i in range(len(query)):
                        # In error case the context manager will ROLLBACK the whole transaction
                        cur.execute(query[i], params[i])
                else:
                    cur.execute(query, params)
                conn.commit()
                if return_type == 'one':
                    ret = cur.fetchone()
                elif return_type == 'all':
                    ret = cur.fetchall()
                else:
                    ret = True
        except psycopg_pool.PoolTimeout:
            print("Error: Connection pool timeout. Couldn't get a connection after the specified timeout.")
            ret = None
        except Exception as e:
            print(f"Error occurred: {str(e)}")
            ret = None

        return ret

    def query(self, query, params=None, row_factory=None):
        return self.__query(query, params=params, return_type=None, row_factory=row_factory)

    def fetch_one(self, query, params=None, row_factory=None):
        return self.__query(query, params=params, return_type='one', row_factory=row_factory)

    def fetch_all(self, query, params=None, row_factory=None):
        return self.__query(query, params=params, return_type='all', row_factory=row_factory)

    def copy_from(self, file, table, columns, sep=','):
        with self._pool.connection() as conn:
            conn.autocommit = False # is implicit default
            cur = conn.cursor()
            statement = f"COPY {table}({','.join(list(columns))}) FROM stdin (format csv, delimiter '{sep}')"
            with cur.copy(statement) as copy:
                copy.write(file.read())


if __name__ == '__main__':
    db = DB()
    result = db.fetch_all('SELECT * FROM runs')
    if result is not None:
        print(result)
    else:
        print("Failed to execute query.")
