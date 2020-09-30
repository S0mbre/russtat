# -*- coding: utf-8 -*-
# --------------------------------------------------------------- #

import os, sys
import json
from rsengine import Russtat, DEBUGGING
from psdb import Psdb, DatabaseError

# --------------------------------------------------------------- #

def main():
    global DEBUGGING

    DEBUGGING = True
    db = Psdb()        

    def add2db(ds):
        nonlocal db        
        ds_json = json.dumps(ds, ensure_ascii=False)
        cur = db()
        try:
            params = [ds_json, 'YYYY-MM-DD HH24-MI-SS', 0, -1, -1]
            cur.callproc('add_data', params)
            print("Added: {}, Data ID = {}, Dataset ID = {}".format(*params[2:]))
        except (Exception, DatabaseError) as err:
            print(err)

    DEBUGGING = False
    rs = Russtat(update_list=False)

    print(f":: {len(rs)} datasets")
    res = rs.get_many(rs[:4], loadfromjson=None, on_dataset=add2db, on_error=print)
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