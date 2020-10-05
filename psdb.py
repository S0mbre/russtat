# -*- coding: utf-8 -*-
# Copyright: (c) 2020, Iskander Shafikov <s00mbre@gmail.com>
# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## @package russtat.psdb
# @brief PostgreSQL manipulation class.
import psycopg2
from psycopg2 import DatabaseError
from globs import DEBUGGING

# --------------------------------------------------------------- # 

## PostgreSQL database engine.
class Psdb:

    ## @param dbname `str` name / path of the Postgres DB on the server
    # @param user `str` Postgres DB user name (default = 'postgres')
    # @param password `str`|`None` Postgres DB password (default = `None`, means it will be asked)
    # @param host `str` Postgres DB server location (default = localhost)
    # @param port `str` Postgres DB server port (default is 5432)
    def __init__(self, dbname='russtat', user='postgres', password=None, host='127.0.0.1', port='5432'):
        ## `Connection object` DB connection object
        self.con = None  
        ## `tuple` DB connection parameters (saved on successful connection)
        self._connparams = None
        self.connect(dbname=dbname, user=user, password=password, host=host, port=port)

    ## Destructor: ensures safe DB disconnect.
    def __del__(self):
        self.disconnect()

    ## Connects to the DB using the given parameters.
    # @param reconnect `bool` if forced reconnect is required
    # @param dbname `str` name / path of the Postgres DB on the server
    # @param user `str` Postgres DB user name (default = 'postgres')
    # @param password `str`|`None` Postgres DB password (default = None, means it will be asked)
    # @param host `str` Postgres DB server location (default = localhost)
    # @param port `str` Postgres DB server port (default is 5432)
    def connect(self, reconnect=False, dbname='russtat', user='postgres', password=None, host='127.0.0.1', port='5432'):
        if not self.con is None and not reconnect:
            return True
        if password is None:
            password = input('>> Enter password:')
        try:
            self.disconnect()
            self.con = psycopg2.connect(database=dbname, user=user, password=password, host=host, port=port)
            self._connparams = (dbname, user, password, host, port)
            if DEBUGGING: print(f'Connected to {self._connparams[0]} as {self._connparams[1]} at {self._connparams[3]}:{self._connparams[4]}')
            return True
        except Exception as err:
            self.con = None
            print(err)
        return False

    ## Disconnects from the DB.
    def disconnect(self):
        if self.con is None: return True
        try:
            self.con.commit()
            self.con.close()
            self.con = None
            if DEBUGGING: print(f'Disconnected from {self._connparams[0] if self._connparams else "DB"}')
            return True
        except:
            pass
        return False

    ## Executes an SQL / PSQL script / command.
    # @param sql `str` SQL / PSQL script
    # @param exec_params `tuple`|`None` SQL / PSQL arguments or `None` if no arguments
    # @param commit `bool` whether to commit changes after executing
    # @returns `Cursor object` current DB cursor
    def exec(self, sql, exec_params=None, commit=False):
        params = [False] + list(self._connparams) if self._connparams else [False, 'russtat', 'postgres', None, '127.0.0.1', '5432']
        if not self.connect(*params):
            return None
        cur = self.con.cursor()
        try:
            if exec_params:
                cur.execute(sql, exec_params)
            else:
                cur.execute(sql)
            if commit:
                self.con.commit()
            return cur
        except Exception as err:
            print(err)
            return None

    ## Fetches the result(s) of an SQL / PSQL command.
    # @param sql `str` SQL / PSQL script
    # @param fetch `str` one of:
    #   - 'iter': return iterator (cursor)
    #   - 'list': return results as a Python list (of tuples)
    #   - 'one': return single result (tuple)
    # @returns `Iterator`|`list`|`tuple` depending on the `fetch` parameter above
    def fetch(self, sql, fetch='iter'):      
        cur = self.exec(sql)
        if cur is None: return None
        if fetch == 'list':
            return cur.fetchall()
        elif fetch == 'one':
            return cur.fetchone()
        else:
            return cur

    ## Overloaded `bool()` operator returns True if the DB connection is active, False otherwise.
    def __bool__(self):
        return not self.con is None

    ## Overloaded `()` operator returns current cursor object or `None` on failure.
    def __call__(self):
        return None if self.con is None else self.con.cursor()