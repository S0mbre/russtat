# -*- coding: utf-8 -*-
# Copyright: (c) 2020, Iskander Shafikov <s00mbre@gmail.com>
# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## @package russtat.psdb
# @brief PostgreSQL manipulation class.
import psycopg2
from psycopg2 import DatabaseError
import pandas as pd
from globs import NL, report, is_iterable

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
            report(f'Connected to {self._connparams[0]} as {self._connparams[1]} at {self._connparams[3]}:{self._connparams[4]}')
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
            report(f'Disconnected from {self._connparams[0] if self._connparams else "DB"}')
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
    #   - 'dry': dry-run: return SQL query string
    # @returns `Iterator`|`list`|`tuple` depending on the `fetch` parameter above
    def fetch(self, sql, fetch='iter', get_header=False, on_error=print):
        if fetch == 'dry':
            return self.con.cursor().mogrify(sql)
        cur = self.exec(sql, on_error=on_error)
        if cur is None: return None
        foo = {'list': cur.fetchall, 'one': cur.fetchone}
        if fetch in foo:
            return (self._get_column_names(cur), foo[fetch]()) if get_header else foo[fetch]()
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

    def fetch_dataframe(self, sql, on_error=print):
        res = self.fetch_dict(sql, on_error)
        return pd.DataFrame(res) if res else None

    def sqlquery(self, table, columns='*', distinct=True, joins=None, condition=None, conj='and',
                groupby=None, having=None, window=None, union=None, orderby=None,
                limit=None, offset=None,
                schema='public', fetch='iter', **kwargs):
        if is_iterable(columns): columns = ', '.join(columns)
        if is_iterable(condition): 
            condition = f' {conj} '.join(f"({c})" for c in condition)
        elif condition:
            condition = f"({condition})"
        condition = f"where {condition}" if condition else ''
        distinct = ' distinct' if distinct else ''
        limit = f'limit {limit}' if limit else ''
        offset = f'offset {offset}' if offset else ''
        joins = '\n'.join(joins) if is_iterable(joins) else (joins or '')
        if groupby:
            if is_iterable(groupby): 
                groupby = ', '.join(groupby)
            groupby = f"group by {groupby}"
        else:
            groupby = ''
        if having:
            if is_iterable(having): 
                having = ', '.join(having)
            having = f"having {having}"
        else:
            having = ''
        window = window or ''
        union = union or ''
        if orderby:
            if is_iterable(orderby): 
                orderby = ', '.join(orderby)
            orderby = f"order by {orderby}"
        else:
            orderby = ''
        if schema: table = f'{schema}.{table}'

        q = f"select{distinct} {columns} from {table} {joins} {condition} "\
            f"{groupby} {having} {window} {union} {orderby} "\
            f"{limit} {offset}".strip() + ';'
        while '  ' in q: q = q.replace('  ', ' ')

        if fetch in ('dict', 'dataframe'):
            foo = self.fetch_dict if fetch == 'dict' else self.fetch_dataframe
            kwargs = {k: v for k, v in kwargs.items() if k in ['sql', 'on_error']}
        else:
            foo = self.fetch

        return foo(q, **kwargs)

    def _get_column_names(self, cur):
        return tuple(c.name for c in cur.description) if cur else tuple()

    def _get_dbtables(self, **kwargs):
        return self.sqlquery('dbtables', **kwargs)
    
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
        if not data_json:
            report('NONE data!', force=True)
            return None
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

    def get_classificator(self, ignore_root=True):
        dsets = self.sqlquery('all_datasets', columns=['classifier', 'id'], condition="classifier <> ''", orderby='classifier')
        spl = [(tuple(s.strip() for s in x[0].split('/')[int(ignore_root):]), x[1]) for x in dsets]
        st = set()
        for el in spl:
            for i in range(1, len(el[0]) + 1):
                st.add(el[0][:i])
        st = sorted(st)
        tout = []
        for t_new in st:
            x = (t_new, [])
            for t_src in spl:
                l = len(t_new)
                if l > len(t_src[0]): continue
                if t_src[0][:l] == t_new:
                    x[1].append(t_src[1])
            tout.append(x)
        return tout

    def print_classificator(self, ignore_root=True, max_categories=None, print_names=True, print_ids=True, max_ds=10, file=None):
        def pr(w):
            if file is None:
                print(w)
            else:
                print(w, file=file)

        lst = self.get_classificator(ignore_root)
        l = {}
        max_categories = max_categories if isinstance(max_categories, int) else len(lst)
        for el in lst[:max_categories]:
            for i in range(len(el[0])):
                if l.get(i, '') == el[0][i]:
                    continue            
                pr(f"{'  ' * i}{el[0][i]}")
                l[i] = el[0][i]
            le = len(el[1])
            if le:
                trunc = isinstance(max_ds, int) and le > max_ds
                dsets = el[1][:max_ds] if trunc else el[1]
                if print_names:
                    dsnames = (x[0] for x in self.get_datasets_by_ids(dsets, columns='dataset'))
                    dsets = zip(dsets, dsnames)
                for ds in dsets:
                    if print_ids:
                        pr("==> {}: {}".format(*ds) if print_names else f"==> {ds}")
                    elif print_names:
                        pr(f"==> {ds[1]}")
                if trunc:
                    pr('==> ...')
                pr(f"[TOTAL {le} DATASETS]")
            else:
                pr('[NO DATASETS]')

    def get_datasets_by_ids(self, ids, **kwargs):
        if ids:
            return self.get_datasets(condition=f"id in ({repr(ids)[1:-1]})", **kwargs)
        else:
            return self.get_datasets(**kwargs)
    
    def get_datasets_by_name(self, pattern, fullmatch=False, case_sensitive=False, **kwargs):
        ds, pat = ('dataset', pattern) if case_sensitive else ('lower(dataset)', pattern.lower())
        if fullmatch:
            return self.get_datasets(condition=f"{ds} = $${pat}$$", **kwargs)
        else:
            return self.get_datasets(condition=f"{ds} like $$%{pat}%$$", **kwargs)

    def get_dataset_info(self, id):
        d = self.sqlquery('all_datasets', condition=f"id = {id}", limit=1, fetch='dict')
        if d:
            dd = {k: v[0] for k, v in d.items()}
            return dd
        return None

    def get_data_by_dataset_id(self, ds_id, extended=False, **kwargs):
        return self.sqlquery('all_data' if extended else 'data_lite', condition=f"ds_id = {ds_id}", **kwargs)

    def get_colnames_datasets(self):
        return self._get_dbtables(columns='column_name::text', condition="table_name::name = 'all_datasets'", fetch='list')

    def get_colnames_data(self):
        return self._get_dbtables(columns='column_name::text', condition="table_name::name = 'data_lite'", fetch='list')

    def get_colnames_data_extended(self):
        return self._get_dbtables(columns='column_name::text', condition="table_name::name = 'all_data'", fetch='list')
