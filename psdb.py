# -*- coding: utf-8 -*-
# Copyright: (c) 2020, Iskander Shafikov <s00mbre@gmail.com>
# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## @package russtat.psdb
# @brief PostgreSQL manipulation class.
from pony.orm import *
from globs import *

# --------------------------------------------------------------- # 

## PostgreSQL database engine.
class Psdb:

    ## @param dbname `str` name / path of the Postgres DB on the server
    # @param user `str` Postgres DB user name (default = 'postgres')
    # @param password `str`|`None` Postgres DB password (default = `None`, means it will be asked)
    # @param host `str` Postgres DB server location (default = localhost)
    # @param port `str` Postgres DB server port (default is 5432)
    def __init__(self, dbname, user='postgres', password='postgres', host='127.0.0.1', port='5432'):
        self.db = Database()
        ## `tuple` DB connection parameters (saved on successful connection)
        self._connparams = None
        self.connect(dbname, user, password, host, port)

    ## Destructor: ensures safe DB disconnect.
    def __del__(self):
        self.disconnect()

    ## Connects to the DB using the given parameters.
    # @param dbname `str` name / path of the Postgres DB on the server
    # @param user `str` Postgres DB user name (default = 'postgres')
    # @param password `str`|`None` Postgres DB password (default = None, means it will be asked)
    # @param host `str` Postgres DB server location (default = localhost)
    # @param port `str` Postgres DB server port (default is 5432)
    def connect(self, dbname, user='postgres', password='postgres', host='127.0.0.1', port='5432'):
        self._connparams = dict(provider='postgres', database=dbname, user=user, password=password, host=host, port=port, create_db=False)
        self.db.bind(**self._connparams)
        return True

    ## Disconnects from the DB.
    def disconnect(self):
        self.db.disconnect()
        return True    

# --------------------------------------------------------------- # 

class Russtatdb(Psdb):

    def __init__(self, dbname='russtat', user='postgres', password=None, host='127.0.0.1', port='5432'):
        super().__init__(dbname, user, password, host, port)
        self._define_tables()

    def _define_tables(self):
        class Agencies(self.db.Entity):
            #_table_ = ("public", "agencies")
            id = PrimaryKey(int, auto=True)
            ag_id = Optional(str, 32)
            name = Required(str, unique=True)  
            # foreigns          
            departments = Set('Departments')
            datasets = Set('Datasets')
            # no support for search(tsvector)!

        class Departments(self.db.Entity):
            #_table_ = ("public", "departments")
            id = PrimaryKey(int, auto=True)           
            name = Required(str)  
            # foreigns          
            agency = Required(Agencies, fk_name='departments_fk1')
            datasets = Set('Datasets')
            # unique
            composite_key(name, agency)
            # no support for search(tsvector)!

        class Classifier(self.db.Entity):
            #_table_ = ("public", "classifier")
            id = PrimaryKey(int, auto=True)
            class_id = Optional(str, 32) 
            name = Required(str, unique=True)
            parent_id = Optional(int) 
            # foreigns
            datasets = Set('Datasets')
            # no support for search(tsvector)!
            
        class Codes(self.db.Entity):
            #_table_ = ("public", "codes")
            id = PrimaryKey(int, auto=True)           
            name = Required(str, unique=True)
            # foreigns            
            codevals = Set('Codevals')
            datasets = Set('Datasets')
            # no support for search(tsvector)! 
        
        class Codevals(self.db.Entity):
            #_table_ = ("public", "codevals")
            id = PrimaryKey(int, auto=True)  
            val_id = Required(str, 64)
            name = Required(str) 
            # foreigns      
            code = Required(Codes, fk_name='codevals_fk1')
            obs = Set('Obs')
            # unique
            composite_key(val_id, code)
            # no support for search(tsvector)! 

        class Periods(self.db.Entity):
            #_table_ = ("public", "periods")
            id = PrimaryKey(int, auto=True)           
            val = Required(str, 256, unique=True)
            # foreigns
            datasets = Set('Datasets')
            obs = Set('Obs')  
            # no support for search(tsvector)! 

        class Units(self.db.Entity):
            #_table_ = ("public", "units")
            id = PrimaryKey(int, auto=True)
            val = Required(str, 256, unique=True)
            # foreigns
            datasets = Set('Datasets')
            obs = Set('Obs')
            # no support for search(tsvector)!

        class Datasets(self.db.Entity):
            #_table_ = ("public", "datasets")
            id = PrimaryKey(int, auto=True)           
            prep_time = Optional(datetime)
            updated_time = Optional(datetime)
            next_update_time = Optional(datetime)
            ds_id = Required(str, unique=True)
            name = Required(str)
            range_start = Optional(int, size=16)
            range_end = Optional(int, size=16)
            description = Optional(str)
            prep_by = Optional(str)
            prep_contact = Optional(str)
            # foreigns
            agency = Required(Agencies, fk_name='datasets_fk1')
            department = Required(Departments, fk_name='datasets_fk2')
            unit = Required(Units, fk_name='datasets_fk3')
            classifier = Required(Classifier, fk_name='datasets_fk4')
            code = Optional(Codes)
            period = Optional(Periods)
            obs = Set('Obs')
            # no support for search(tsvector)! 

        class Obs(self.db.Entity):
            #_table_ = ("public", "obs")
            id = PrimaryKey(int, 64, auto=True)           
            obs_year = Required(int)
            obs_val = Required(float)
            # foreigns
            dataset = Required(Datasets, fk_name='obs_fk1')
            codeval = Required(Codevals, fk_name='obs_fk2')
            unit = Required(Units, fk_name='obs_fk3')
            period = Required(Periods, fk_name='obs_fk4')
            # unique
            composite_key(obs_year, dataset, codeval, unit, period)

        self.db.generate_mapping(create_tables=False)    

    @db_session
    def findin_datasets(self, query):
        """
        RETURNS:
        id integer, classificator text, dsname text, updated timestamp with time zone, 
        preptime timestamp with time zone, nextupdate timestamp with time zone, 
        description text, agency text, department text, startyr smallint, 
        endyr smallint, prepby text, contact text, ranking real
        """
        return self.db.select(f"select * from search_datasets($${query}$$::text)")

    @db_session
    def findin_data(self, query):
        """
        RETURNS:
        id bigint, classificator text, dsname text, description text, 
        preptime timestamp with time zone, updated timestamp with time zone, 
        nextupdate timestamp with time zone, agency text, department text, 
        startyr smallint, endyr smallint, prepby text, contact text, 
        obsyear integer, obsperiod character varying, obsunit character varying, 
        obscode text, obscodeval text, value real, ranking real
        """
        return self.db.select(f"select * from search_data($${query}$$::text)")

    @db_session
    def get_datasets(self):
        return self.db.select(f"select * from all_datasets")

    @db_session
    def get_data(self, **kwargs):
        return self.db.select(f"select * from all_data")
    
    @db_session
    def add_data(self, data_json, disable_triggers=False):
        triggers_disabled = False
        if disable_triggers:
            triggers_disabled = self.disable_triggers()
        res = self.db.get(f"select * from public.add_data($${data_json}$$::text);")
        if triggers_disabled:
            self.enable_triggers()
        return res

    @db_session
    def disable_triggers(self):
        return bool(self.db.execute("call public.disable_triggers();"))

    @db_session
    def enable_triggers(self, reindex=True):
        return bool(self.db.execute(f"call public.enable_triggers({int(reindex)}::boolean);"))

    @db_session
    def clear_all_data(self, full_clear=False, confirm_action=None, on_error=print):
        if confirm_action and not confirm_action():
            return False
        return bool(self.db.execute(f"call public.clear_all({int(full_clear)}::boolean);"))