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
# @param dbname `str` name / path of the Postgres DB on the server
# @param user `str` Postgres DB user name (default = 'postgres')
# @param password `str`|`None` Postgres DB password (default = `None`, means it will be asked)
# @param host `str` Postgres DB server location (default = localhost)
# @param port `str` Postgres DB server port (default is 5432)
# @param logfile `str`|`None` Output stream for notification messages; 
# `None` means STDOUT, otherwise, a valid path is expected
def add2db(ds, dbname='russtat', user='postgres', password=None, 
           host='127.0.0.1', port='5432', logfile=None):   

    # by default messages are printed to the console
    if logfile is None:
        logfile = sys.stdout
    else:
        # ...but a different file may be indicated
        try:
            logfile = open(os.path.abspath(logfile), 'a', encoding='utf-8')
        except:
            logfile = sys.stdout

    try:
        # create DB object and connect with provided parameters
        db = Psdb(dbname, user, password, host, port)
        # dump dataset to JSON string
        ds_json = json.dumps(ds, ensure_ascii=False, default=str)
        # add data passing the JSON string into the server function 'add_data'
        cur = db.exec(f"select * from public.add_data($${ds_json}$$::text);", commit=True)
        # result must be a valid Cursor object
        if cur:
            # fetch results (added count, last data ID, last dataset ID)
            res = cur.fetchall()
            # notify on successful insert operation
            if res:                
                print("{}\n\tAdded: {}, Data ID = {}, Dataset ID = {}".format(
                      ds['full_name'], res[0][0], res[0][1], res[0][2]), 
                      end='\n\n', file=logfile, flush=True)
            else: 
                # print server messages and raise error             
                raise Exception('\n'.join(db.con.notices))
        else:
            # print server messages and raise error 
            raise Exception('\n'.join(db.con.notices))

    except (Exception, DatabaseError) as err:
        # print error message
        print("{}\n\t{}".format(ds['full_name'], err), end='\n\n', file=logfile, flush=True)

    except:
        # print traceback info from stack
        traceback.print_exc(limit=None, file=logfile)

    finally:
        # close output file, if any
        if logfile and logfile != sys.stdout:
            logfile.close()

## Main function that creates the Russtat engine and retrieves / stores data.
def main(update_list=False, start_ds=0, end_ds=-1, pwd=None, logfile=None): 
    # create data retrieving engine
    update_list = bool(update_list)
    rs = Russtat(update_list=update_list)
    # print number of available datasets
    print(f":: {len(rs)} datasets")

    # ask DB password
    dbpassword = input('Enter DB password:') if pwd is None else pwd
    # start operation using multiple processes
    start_ds = int(start_ds)
    end_ds = int(end_ds)
    if end_ds == -1: end_ds = len(rs)
    res = rs.get_many(rs[start_ds:end_ds], 
                      on_dataset=add2db, save2json=None, loadfromjson='auto', del_xml=True,
                      on_dataset_kwargs={'password': dbpassword, 'logfile': logfile}, 
                      on_error=print).get()
    # print summary
    if res:
        cnt = len(res)
        nonval = sum(1 for x in res if x is None) 
        print(f":: Processed {cnt} datasets: {cnt - nonval} valid, {nonval} non-valid")
    else:
        print(':: No datasets processed!')

# --------------------------------------------------------------- #

## Program entry point.
if __name__ == '__main__':
    args = sys.argv
    L = len(args)
    if L == 1:
        main()
    else:
        main(*args[1:])