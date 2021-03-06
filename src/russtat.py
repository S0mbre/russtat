# -*- coding: utf-8 -*-
# Copyright: (c) 2020, Iskander Shafikov <s00mbre@gmail.com>
# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## @package russtat.russtat
# @brief Application entry point.
import os, sys, json, traceback
from datetime import datetime
from rsengine import Russtat
from psdb import Russtatdb
from globs import timeit

# --------------------------------------------------------------- #

## Callback procedure for dataset processing: loads dataset into PSQL database.
# @param ds `dict` The stats dataset as a dictionary object -- see rsengine::Russtat::get_one()
# @param db `psdb::Russtatdb` | `None` DB connection object; may be `None` to connect locally
# @param dbparams `dict` DB connection parameters passed to the `psdb::Russtatdb` constructor
# `None` means STDOUT, otherwise, a valid path is expected
def add2db(ds, db=None, dbparams={}, logfile=None):   

    # by default messages are printed to the console
    closelog = False
    if logfile is None:
        logfile = sys.stdout
    elif isinstance(logfile, str):
        # ...but a different file may be indicated
        try:
            logfile = open(os.path.abspath(logfile), 'a', encoding='utf-8')
            closelog = True
        except:
            logfile = sys.stdout

    try:
        if db is None:
            # create DB object and connect with provided parameters
            db = Russtatdb(**dbparams)

        # dump dataset to JSON string and pass into the server function 'add_data'
        res = db.add_data(json.dumps(ds, ensure_ascii=False, default=str))

        # result must be a 3-tuple
        if res:
            print("'{}'\n\t{}\tAdded: {}, Data ID = {}, Dataset ID = {}".format(
                    ds['full_name'], f"{datetime.now():'%b.%d %H:%M:%S'}", 
                    res[0], res[1], res[2]), end='\n\n', file=logfile, flush=True)
        else:
            raise Exception('FAILED TO IMPORT')

    except Exception as err:
        # print error message
        print("{}\n\t{}".format(ds['full_name'], err), end='\n\n', file=logfile, flush=True)

    except:
        # print traceback info from stack
        traceback.print_exc(limit=None, file=logfile)

    finally:
        # close output file, if any
        if closelog: logfile.close()

# --------------------------------------------------------------- #            

## Updates the database.
# @param update_list `bool` whether to refresh the dataset list from the server
# @param start_ds `int` start dataset index to upload to DB
# @param end_ds `int` last dataset index to upload to DB (`-1` = no limit)
# @param skip_existing `bool` set to `True` to skip existing datasets (on title coincidence)
# @param pwd `str` | `None` database password (leave `None` to ask user)
# @param logfile `str` | `None` output file to print messages (`None` = STDOUT)
@timeit
def update_db(update_list=False, start_ds=0, end_ds=-1, skip_existing=True, pwd=None, disable_triggers=True, logfile=None): 
    # create data retrieving engine
    update_list = int(update_list)
    rs = Russtat(update_list=update_list)
    
    # ask DB password
    dbpassword = input('Enter DB password:') if pwd is None else pwd

    # connect to DB
    db = Russtatdb(password=dbpassword)    

    try:
        # start operation using multiple processes
        start_ds = int(start_ds)
        end_ds = int(end_ds)
        if end_ds == -1: end_ds = len(rs)
        datasets = rs[start_ds:end_ds]
        if int(skip_existing):
            datasets = rs.filter_datasets(db, datasets, 'new')
        if not logfile:
            logfile = None

        # print number of available and new datasets
        print(f":: {len(rs)} datasets / {len(datasets)} ({int(len(datasets) * 100.0 / len(rs))}%) to add/update.")
        #for ds in datasets: print(ds)
        #return

        if not datasets:
            print(f":: NO DATASETS TO UPDATE!")
            db.disconnect()
            return

        # disable triggers to speed up process
        if int(disable_triggers):
            print(f":: Disabling DB triggers...")
            triggers_disabled = db.disable_triggers() 
        else:
            triggers_disabled = False

        try:
            print(f":: Processing {len(datasets)} datasets...")

            res = rs.get_many(datasets, del_xml=True, save2json=None, loadfromjson='auto', on_dataset=add2db, 
                              on_dataset_kwargs={'dbparams': {'password': dbpassword}, 'logfile': logfile}, 
                              on_error=print)
            # get results
            if res:                
                res = res.get()

                if res:                    
                    # print summary
                    cnt = len(res)
                    nonval = sum(1 for x in res if x is None) 
                    print(f":: Processed {cnt} datasets: {cnt - nonval} valid, {nonval} non-valid")

                else:
                    print(':: No datasets processed!')

            else:
                print(':: Error executing operation!')

        finally:
            # re-enable triggers
            if triggers_disabled: 
                print(f":: Re-enabling DB triggers and updating vector indices...")
                db.enable_triggers()

    finally:
        db.disconnect()

def testing():
    dbpassword = input('Enter DB password:')
    db = Russtatdb(password=dbpassword)

    db.print_classificator(max_categories=50, print_ids=False, max_ds=5) 
    return

    # example 1: simple data query
    res = db.get_data(condition="dataset like '%комит%' and year = 2018", limit=30, get_header=True)
    if res:
        print(res[0], end='\n'*2)
        for row in res[1]:
            print(row, end='\n'*2)
        print('-' * 30)

    # example 2: full-text search and convertion to pandas DataFrame
    res = db.get_data(condition="dataset like '%комит%' and year = 2018", limit=30, fetch='dataframe')
    if not res is None:        
        print(res.loc[:, 'prepared':'agency'])
        print('-' * 30)

    # example 3: raw SQL query
    res = db.sqlquery('datasets', columns=['name', 'description'], 
                    condition="name like '%зарегистрированных%'", limit=10)
    if res:
        for row in res:
            print(row, end='\n'*2)
        print('-' * 30)

# --------------------------------------------------------------- #

## Main function that creates the Russtat engine and retrieves / stores data.
def main():

    # if len(sys.argv) == 1:
    #     update_db()
    # else:
    #     update_db(*sys.argv[1:])

    testing()

# --------------------------------------------------------------- #

## Program entry point.
if __name__ == '__main__':
    main()