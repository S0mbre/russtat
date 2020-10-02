# -*- coding: utf-8 -*-
# Copyright: (c) 2020, Iskander Shafikov <s00mbre@gmail.com>
# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## @package russtat.russtat
# @brief Application entry point.
import os, sys
import json
from rsengine import Russtat, DEBUGGING
from psdb import Psdb, DatabaseError

# --------------------------------------------------------------- #

## Callback procedure for dataset processing: loads dataset into PSQL database.
# @param ds `dict` The stats dataset as a dictionary object -- see rsengine::Russtat::get_one()
# @param password `str` The database password (default = postgres)
def add2db(ds, password):   
    db = Psdb(password=password)
    ds_json = json.dumps(ds, ensure_ascii=False, default=str)
    try:
        cur = db.exec(f"select * from public.add_data('{ds_json}'::text);", commit=True)
        if cur:
            res = cur.fetchall()
            if res:       
                print("Added: {}, Data ID = {}, Dataset ID = {}".format(*res[0]))
            else:                
                raise Exception('\n'.join(db.con.notices))
        else:
            raise Exception('\n'.join(db.con.notices))
    except (Exception, DatabaseError) as err:
        print(err)
    except:
        print('Some error')

## Main function that creates the Russtat engine and retrieves / stores data.
def main(): 
    rs = Russtat(update_list=False)
    print(f":: {len(rs)} datasets")

    dbpassword = input('Enter DB password:')
    res = rs.get_many(rs[23:26], on_dataset=add2db, on_dataset_kwargs={'password': dbpassword}, 
                      on_error=print)
    print(f":: Processed {len(res.get())} datasets")  

# --------------------------------------------------------------- #

## Program entry point.
if __name__ == '__main__':
    main()