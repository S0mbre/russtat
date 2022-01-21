# -*- coding: utf-8 -*-
# Copyright: (c) 2020, Iskander Shafikov <s00mbre@gmail.com>
# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## @package russtat.psdb
# @brief PostgreSQL manipulation class.
import psycopg2
from psycopg2 import DatabaseError
import pandas as pd
from globs import NL, report, is_iterable, sys

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
            return [self.con.cursor().mogrify(sql).decode('utf-8')]
        cur = self.exec(sql, on_error=on_error)
        if cur is None: return None
        foo = {'list': cur.fetchall, 'one': cur.fetchone}
        if fetch in foo:
            return (self._get_column_names(cur), foo[fetch]()) if get_header else foo[fetch]()
        else:
            return (self._get_column_names(cur), cur) if get_header else cur

    def data_to_dict(self, data, on_error=print):
        if data is None: return None
        
        if 'cursor' in str(type(data)).lower():
            names = self._get_column_names(data)
            rows = data
        elif isinstance(data, tuple):
            names = data[0]
            rows = data[1]
        elif on_error:
            on_error('data parameter must be a DB cursor object or a tuple (column names, values)')
            return None

        vals = {n: [] for n in names}
        for row in rows:
            for i, n in enumerate(names):
                vals[n].append(row[i])
        return vals

    def data_to_dataframe(self, data, on_error=print):
        d = self.data_to_dict(data, on_error)
        return pd.DataFrame(d) if d else None

    def fetch_dict(self, sql, on_error=print):
        cur = self.exec(sql, on_error=on_error)
        return self.data_to_dict(cur, on_error)

    def fetch_dataframe(self, sql, on_error=print):
        cur = self.exec(sql, on_error=on_error)
        return self.data_to_dataframe(cur, on_error)

    def iterate_data(self, colnames, data):
        if not data: return
        for row in data:
            yield zip(colnames, row)

    def iterate_data_formatted(self, colnames, data, keysep=': ', 
                               colname_handler=None, value_handler=None, 
                               filler='========================================',
                               number_handler=None):
        if not data: return
        i = 0
        for row in data:
            if number_handler:
                yield(number_handler(i))
            for tup in zip(colnames, row):
                c = colname_handler(tup[0]) if colname_handler else tup[0]
                v = value_handler(tup[1]) if value_handler else tup[1]
                yield f"{c}{keysep}{v}"
            if filler: yield filler
            i += 1

    def sqlquery(self, table, columns='*', distinct=True, joins=None, condition=None, conj='and',
                groupby=None, having=None, window=None, union=None, orderby=None,
                limit=None, offset=None,
                schema='public', **kwargs):
        fetch = kwargs.get('fetch', 'iter')
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

        #print(q)
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

    def findin_datasets(self, query, extended=False, **kwargs):
        """
        RETURNS:
        id integer, classificator text, dsname text, updated timestamp with time zone, 
        preptime timestamp with time zone, nextupdate timestamp with time zone, 
        description text, agency text, department text, startyr smallint, 
        endyr smallint, prepby text, contact text, ranking real
        """
        return self.sqlquery(f"search_datasets{'_web' if not extended else ''}($${query}$$::text)", **kwargs)

    def findin_data(self, query, extended=False, **kwargs):
        """
        RETURNS:
        id bigint, classificator text, dsname text, description text, 
        preptime timestamp with time zone, updated timestamp with time zone, 
        nextupdate timestamp with time zone, agency text, department text, 
        startyr smallint, endyr smallint, prepby text, contact text, 
        obsyear integer, obsperiod character varying, obsunit character varying, 
        obscode text, obscodeval text, value real, ranking real
        """
        return self.sqlquery(f"search_data{'_web' if not extended else ''}($${query}$$::text)", **kwargs)

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

    def get_classificator2(self, ignore_root=True):
        dsets = self.sqlquery('all_datasets', columns=['classifier', 'id'], condition="classifier <> ''", orderby='classifier')
        

    def get_classificator(self, ignore_root=True, max_levels=None):
        dsets = self.sqlquery('all_datasets', columns=['classifier', 'id'], condition="classifier <> ''", orderby='classifier')
        spl = [(tuple(s.strip() for s in x[0].split('/')[int(ignore_root):max_levels]), x[1]) for x in dsets]
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

    def collect_classificator(self, ignore_root=True, max_levels=None, max_categories=None):
        lst = self.get_classificator(ignore_root, max_levels)
        l = {}
        results = []
        for el in lst[:max_categories]:
            le = len(el[1])

            j = 0
            for i in range(len(el[0])):
                if l.get(i, '') == el[0][i]:
                    continue            
                results.append({'level': i, 'name': el[0][i], 'count': le, 'id': -1})
                l[i] = el[0][i]
                j = i + 1    

            if le:                
                dsets = el[1]
                dsnames = (x[0] for x in self.get_datasets_by_ids(dsets, columns='dataset'))
                dsets = zip(dsets, dsnames)

                for ds in dsets:
                    results.append({'level': j, 'name': ds[1], 'count': 1, 'id': ds[0]})
        
        return results

    def output_classificator(self, ignore_root=True, max_levels=None, max_categories=None, 
                             print_names=True, print_ids=True, max_ds=10, indent='  '):
        lst = self.get_classificator(ignore_root, max_levels)
        l = {}
        for el in lst[:max_categories]:
            for i in range(len(el[0])):
                if l.get(i, '') == el[0][i]: continue            
                yield f"{indent * i}{el[0][i]}"
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
                        yield f"{indent * (i + 1)}{ds[0]}: {ds[1]}" if print_names else f"{indent * i}{ds}"
                    elif print_names:
                        yield f"{indent * (i + 1)}{ds[1]}"
                if trunc:
                    yield f'{indent * (i + 1)} ...'

    def print_classificator(self, file=None, **kwargs):
        if not file: file = sys.stdout
        for row in self.output_classificator(**kwargs):
            print(row, file=file)

    def get_datasets_by_ids(self, ids, **kwargs):
        if ids:
            return self.get_datasets(condition=f"id in ({repr(ids)[1:-1]})", **kwargs)
        else:
            return self.get_datasets(**kwargs)
    
    def get_datasets_by_name(self, pattern, field='dataset', fullmatch=False, case_sensitive=False, **kwargs):
        ds, pat = (field, pattern) if case_sensitive else (f'lower({field})', pattern.lower())
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
