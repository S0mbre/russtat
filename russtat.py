# -*- coding: utf-8 -*-
# --------------------------------------------------------------- #

import os, sys
import json
from rsengine import Russtat, DEBUGGING
from psdb import Psdb, DatabaseError

# --------------------------------------------------------------- #
dbpassword = None
# --------------------------------------------------------------- #

def add2db(ds):    
    db = Psdb(password='Fknzoo2052')
    ds_json = json.dumps(ds, ensure_ascii=False, default=str)
    try:
        cur = db.exec('CALL add_data(%s, %s, %s, %s, %s)', 
                     (ds_json, 'YYYY-MM-DD HH24-MI-SS', 0, -1, -1), True)
        res = cur.fetchone()
        if res:
            res = res[0]
            print("Added: {}, Data ID = {}, Dataset ID = {}".format(*res))
    except (Exception, DatabaseError) as err:
        print(err)
    except:
        print('Some error')

def main(): 
    #global dbpassword
    rs = Russtat(update_list=False)

    #print(f":: {len(rs)} datasets")
    #dbpassword = input('Enter DB password:')
    res = rs.get_many(0, loadfromjson=None, on_dataset=add2db, on_error=print)
    print(f":: Processed {len(res.get())} datasets")  

# --------------------------------------------------------------- #

if __name__ == '__main__':
    main()