# -*- coding: utf-8 -*-
# Copyright: (c) 2020, Iskander Shafikov <s00mbre@gmail.com>
# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## @package russtat.psdb
# @brief PostgreSQL manipulation class.
import psycopg2
from psycopg2 import DatabaseError
from globs import *

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
    def exec(self, sql, exec_params=None, commit=False, on_error=print):
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
        except (Exception, DatabaseError) as err:
            if on_error: 
                on_error(f"{str(err)}{NL}ORIGINAL QUERY:{NL}{cur.query.decode('utf-8')}")
            return None

    ## Fetches the result(s) of an SQL / PSQL command.
    # @param sql `str` SQL / PSQL script
    # @param fetch `str` one of:
    #   - 'iter': return iterator (cursor)
    #   - 'list': return results as a Python list (of tuples)
    #   - 'one': return single result (tuple)
    # @returns `Iterator`|`list`|`tuple` depending on the `fetch` parameter above
    def fetch(self, sql, fetch='iter', get_header=False, on_error=print):
        cur = self.exec(sql, on_error=on_error)
        if cur is None: return None
        if fetch == 'list':
            return (self._get_column_names(cur), cur.fetchall()) if get_header else cur.fetchall()
        elif fetch == 'one':
            return (self._get_column_names(cur), cur.fetchone()) if get_header else cur.fetchone()
        else:
            return (self._get_column_names(cur), cur) if get_header else cur

    def fetch_dict(self, sql, on_error=print):
        cur = self.exec(sql, on_error=on_error)
        if cur is None: return None
        names = self._get_column_names(cur)
        data = {n: [] for n in names}
        for row in cur:
            for i, n in enumerate(names):
                data[n].append(row[i])
        return data

    def sqlquery(self, table, columns='*', joins=None, condition=None, limit=None, 
                schema='public', as_dict=False, **kwargs):
        if is_iterable(columns): columns = ', '.join(columns)
        condition = f"where ({condition})" if condition else ''
        limit = f'limit {limit}' if limit else ''
        joins = '\n'.join(joins) if is_iterable(joins) else (joins or '')
        if schema: table = f'{schema}.{table}'
        if as_dict:
            foo = self.fetch_dict
            kwargs = {k: v for k, v in kwargs.items() if k in ['sql', 'on_error']}
        else:
            foo = self.fetch
        return foo(f"select {columns} from {table}{NL}{joins}{NL}{condition}{NL}{limit};", **kwargs)

    def _get_column_names(self, cur):
        return tuple(c.name for c in cur.description) if cur else tuple()
    
    ## Overloaded `bool()` operator returns True if the DB connection is active, False otherwise.
    def __bool__(self):
        return not self.con is None

    ## Overloaded `()` operator returns current cursor object or `None` on failure.
    def __call__(self):
        return None if self.con is None else self.con.cursor()

# --------------------------------------------------------------- # 

class Russtatdb(Psdb):

    def __init__(self, dbname='russtat', user='postgres', password=None, host='127.0.0.1', port='5432'):
        super().__init__(dbname, user, password, host, port)

    def dbmessages(self, default='Database Error'):
        return '\n'.join(self.con.notices) if self.con.notices else default

    def findin_datasets(self, query, **kwargs):
        """
        RETURNS:
        id integer, classificator text, dsname text, updated timestamp with time zone, 
        preptime timestamp with time zone, nextupdate timestamp with time zone, 
        description text, agency text, department text, startyr smallint, 
        endyr smallint, prepby text, contact text, ranking real
        """
        return self.sqlquery(f"search_datasets($${query}$$::text)", **kwargs)

    def findin_data(self, query, **kwargs):
        """
        RETURNS:
        id bigint, classificator text, dsname text, description text, 
        preptime timestamp with time zone, updated timestamp with time zone, 
        nextupdate timestamp with time zone, agency text, department text, 
        startyr smallint, endyr smallint, prepby text, contact text, 
        obsyear integer, obsperiod character varying, obsunit character varying, 
        obscode text, obscodeval text, value real, ranking real
        """
        return self.sqlquery(f"search_data($${query}$$::text)", **kwargs)

    def get_datasets(self, **kwargs):
        return self.sqlquery('all_datasets', **kwargs)

    def get_data(self, **kwargs):
        return self.sqlquery('all_data', **kwargs)
    
    def add_data(self, data_json, disable_triggers=False, on_error=print):
        triggers_disabled = False
        if disable_triggers:
            triggers_disabled = self.disable_triggers(on_error=on_error)
        cur = self.exec(f"select * from public.add_data($${data_json}$$::text);", commit=True, on_error=on_error)        
        if cur:
            res = cur.fetchone()
            if triggers_disabled:
                self.enable_triggers(on_error=on_error)
            return res
        else:
            raise Exception(self.dbmessages)

    def disable_triggers(self, on_error=print):
        cur = self.exec("call public.disable_triggers();", commit=True, on_error=on_error)
        return True if cur else False

    def enable_triggers(self, reindex=True, on_error=print):
        cur = self.exec(f"call public.enable_triggers({int(reindex)}::boolean);", commit=True, on_error=on_error)
        return True if cur else False

    def clear_all_data(self, full_clear=False, confirm_action=None, on_error=print):
        if confirm_action and not confirm_action():
            return False
        cur = self.exec(f"call public.clear_all({int(full_clear)}::boolean);", commit=True, on_error=on_error)
        return True if cur else False