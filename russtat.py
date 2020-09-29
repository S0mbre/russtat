# -*- coding: utf-8 -*-
# --------------------------------------------------------------- #

import os, sys
from rsengine import Russtat, DEBUGGING
from psdb import Psdb

# --------------------------------------------------------------- #

def main():
    global DEBUGGING

    DEBUGGING = True

    db = Psdb()
    
    DEBUGGING = False

    def add2db(ds):
        nonlocal db
        
        # agency
        sql = """
            insert into agencies(ag_id, name)
            values ({}, {})
            on conflict (name) do nothing; 
            """.format(ds['agency_id'], ds['agency_name'])
        if not db.exec(sql): return
        id_agency = db.fetch("select id from agencies where name = '{}' limit 1;".format(ds['agency_name']), 'one')
        if not id_agency: return
        id_agency = id_agency[0]

        # department
        

    rs = Russtat(update_list=False)

    print(f":: {len(rs)} datasets")
    res = rs.get_many(rs[:10], loadfromjson=None, on_dataset=add2db, on_error=print)
    print(f":: Processed {len(res.get())} datasets")  
    
def m():
    db = Psdb()
    id_ = db.fetch("select id from agencies where name = 'Новое агентство' limit 1;", 'one')
    if id_: 
        id_ = id_[0]
    print(id_)

# --------------------------------------------------------------- #

if __name__ == '__main__':
    main()