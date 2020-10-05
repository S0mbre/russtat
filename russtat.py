# -*- coding: utf-8 -*-
# Copyright: (c) 2020, Iskander Shafikov <s00mbre@gmail.com>
# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## @package russtat.russtat
# @brief Application entry point.
import os, sys, json, traceback
from rsengine import Russtat, DEBUGGING
from psdb import Psdb, DatabaseError

# --------------------------------------------------------------- #

## Callback procedure for dataset processing: loads dataset into PSQL database.
# @param ds `dict` The stats dataset as a dictionary object -- see rsengine::Russtat::get_one()
# @param password `str` The database password (default = postgres)
def add2db(ds, password=None, logfile=None):   
    if logfile is None:
        logfile = sys.stdout
    else:
        try:
            logfile = open(os.path.abspath(logfile), 'a', encoding='utf-8')
        except:
            logfile = sys.stdout

    try:
        db = Psdb(password=password)
        ds_json = json.dumps(ds, ensure_ascii=False, default=str)
    
        cur = db.exec(f"select * from public.add_data($${ds_json}$$::text);", commit=True)
        if cur:
            res = cur.fetchall()
            if res:                
                print("{}\n\tAdded: {}, Data ID = {}, Dataset ID = {}".format(
                      ds['full_name'], res[0][0], res[0][1], res[0][2]), 
                      end='\n\n', file=logfile, flush=True)
            else:                
                raise Exception('\n'.join(db.con.notices))
        else:
            raise Exception('\n'.join(db.con.notices))

    except (Exception, DatabaseError) as err:
        print("{}\n\t{}".format(ds['full_name'], err), end='\n\n', file=logfile, flush=True)

    except:
        traceback.print_exc(limit=None, file=logfile)

    finally:
        if logfile and logfile != sys.stdout:
            logfile.close()

## Main function that creates the Russtat engine and retrieves / stores data.
def main(): 
    rs = Russtat(update_list=False)
    print(f":: {len(rs)} datasets")

    dbpassword = input('Enter DB password:')
    res = rs.get_many(rs[:1], on_dataset=add2db, 
                      on_dataset_kwargs={'password': dbpassword, 'logfile': None}, 
                      on_error=print)
    if res:           
        print(f":: Processed {len(res.get())} datasets")

# --------------------------------------------------------------- #

## Program entry point.
if __name__ == '__main__':
    main()