# -*- coding: utf-8 -*-
# Copyright: (c) 2020, Iskander Shafikov <s00mbre@gmail.com>
# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## @package russtat.russtat
# @brief Application entry point.
from prompt_toolkit import (output, PromptSession, print_formatted_text, formatted_text,
                            history, auto_suggest, shortcuts, styles, key_binding, lexers)
import pygments                    
from pygments.lexers.sql import PostgresLexer

import os, sys, json, traceback
from datetime import datetime
from rsengine import Russtat
from psdb import Russtatdb, pd
from globs import timeit, REPLMENU

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

def msg(title, text, ok_text='OK'):
    return shortcuts.message_dialog(title=title, text=text, ok_text=ok_text).run()

def input_msg(title, text, ok_text='OK', cancel_text='Cancel', password=False):
    return shortcuts.input_dialog(title=title, text=text, ok_text=ok_text, cancel_text=cancel_text, password=password).run()

def yesno_msg(title, text, yes_text='YES', no_text='NO'):
    return shortcuts.yes_no_dialog(title=title, text=text, yes_text=yes_text, no_text=no_text).run()

def print_long_iterator(session, it, max_rows=50):
    n = 0
    for line in it:
        try:        
            n += 1
            print_formatted_text(line)
            if max_rows > 0 and n % max_rows == 0:
                session.prompt('', bottom_toolbar=formatted_text.HTML('----- Hit <violet>Return</violet> to continue, <violet>Ctrl + C</violet> to abort -----'))
        except:
            break

def print_dataframe(session, df, max_rows=50): 
    l = len(df)
    if max_rows <= 0 or l <= max_rows:
        print_formatted_text(df.to_markdown(tablefmt='psql'))
    else:
        parts = divmod(l, max_rows)
        parts = parts[0] + 1 if parts[1] > 0 else parts[0]
        for i in range(parts):
            try:
                print_formatted_text(df.iloc[i * max_rows:(i + 1) * max_rows].to_markdown(tablefmt='psql'))
                session.prompt('', bottom_toolbar=formatted_text.HTML('----- Hit <OrangeRed>Return</OrangeRed> to continue, <OrangeRed>Ctrl + C</OrangeRed> to abort -----'))
            except Exception as err:
                msg('Error', repr(err))
                break

def get_menu(stack):
    nd = REPLMENU
    try:
        for m in stack:
            nd = nd[m]
        return (formatted_text.HTML(nd['MENU'] if isinstance(nd, dict) else nd), nd)
    except:
        return (formatted_text.HTML(REPLMENU['!']['MENU']), REPLMENU['!'])          

def cli():    

    stack = ['!']  
            
    dbpassword = input_msg('DB password', '', password=True)

    db = Russtatdb(password=dbpassword)
    if not db:
        msg('Error', 'DB connection failed!')
        return

    # renderer = output.win32.Win32Output(stdout=sys.stdout, use_complete_width=True)
    # renderer.disable_autowrap()
    # session = PromptSession(output=renderer)
    session = PromptSession()

    text = ''
    while text != 'Q':
        try:
            tb, nd = get_menu(stack)
            text = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}] >> "), 
                                    auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                                    bottom_toolbar=tb)
            txt = text[0]

            if txt == 'Q':
                break

            elif txt == '*':
                if len(stack) > 1:
                    stack.pop()
                else:
                    break

            elif isinstance(nd, dict) and txt in nd:
               
                stack.append(txt)
                # EXEC COMMAND  
                try:
                    path = '/'.join(stack)
                    if path == '!/d/d':
                        data = db.get_datasets(get_header=True)
                        print_long_iterator(session, db.iterate_data_formatted(*data))
                        stack.pop()
                                           
                    elif path == '!/d/f':
                        text1 = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}]: SEARCH >> "), 
                                        auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                                        bottom_toolbar=tb)
                        if text1:
                            data = db.findin_datasets(text1, get_header=True)
                            print_long_iterator(session, db.iterate_data_formatted(*data))
                        stack.pop()
                       
                    elif path == '!/d/c':
                        print_long_iterator(session, (f"{i}. {r[0]}" for (i, r) in enumerate(db.get_colnames_datasets())))
                        stack.pop()
                       
                    elif path == '!/d/p':
                        print_long_iterator(session, db.output_classificator(max_ds=None, indent='....'))
                        stack.pop()

                    elif path == '!/d/s/i':
                        text1 = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}]: IDS (COMMA-SEP) >> "), 
                                        auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                                        bottom_toolbar=tb)
                        if text1:
                            ids = [s.strip() for s in text1.split(',')]
                            data = db.get_datasets_by_ids(ids, get_header=True)
                            print_long_iterator(session, db.iterate_data_formatted(*data))
                        stack.pop()

                    elif path == '!/d/s/n':
                        text1 = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}]: (PART OF) NAME (IN ANY CASE) >> "), 
                                        auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                                        bottom_toolbar=tb)
                        if text1:
                            data = db.get_datasets_by_name(text1, get_header=True)
                            print_long_iterator(session, db.iterate_data_formatted(*data))
                        stack.pop()

                    elif path == '!/d/s/c':
                        text1 = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}]: (PART OF) CATEGORY (IN ANY CASE) >> "), 
                                        auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                                        bottom_toolbar=tb)
                        if text1:
                            data = db.get_datasets_by_name(text1, 'classifier', get_header=True)
                            print_long_iterator(session, db.iterate_data_formatted(*data))
                        stack.pop()

                    elif path == '!/d/s/e/p':
                        params = {
                            'columns': {'prompt': 'COLUMN NAMES (COMMA-SEP) - <u><red>Return</red> to display ALL columns, <red>Q</red> to abort</u>', 'val': '*'}, 
                            'condition': {'prompt': 'CONDITIONS (; SEP) - <u><red>Return</red> to apply NO conditions, <red>Q</red> to abort</u>', 'val': None}, 
                            'orderby': {'prompt': 'ORDER BY (COMMA-SEP) - <u><red>Return</red> to use DEFAULT ordering, <red>Q</red> to abort</u>', 'val': None}, 
                            'limit': {'prompt': 'MAX RESULTS (E.G. 1) - <u><red>Return</red> to apply NO limit, <red>Q</red> to abort</u>', 'val': None}, 
                            'offset': {'prompt': 'FIRST RESULT OFFSET - <u><red>Return</red> = start from first row, <red>Q</red> to abort</u>', 'val': None} 
                        }
                        for p in params:
                            ptxt = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}]: {params[p]['prompt']} >> "), 
                                                    auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                                                    bottom_toolbar=tb)
                            if ptxt == 'Q':
                                break
                            else:
                                if ptxt:
                                    if p == 'condition' and ';' in ptxt:
                                        ptxt = [s.strip() for s in ptxt.split(';')]
                                    elif p in ('columns', 'orderby') and ',' in ptxt:
                                        ptxt = [s.strip() for s in ptxt.split(',')]
                                    params[p]['val'] = ptxt
                        else:                        
                            data = db.get_datasets(get_header=True, **{p: params[p]['val'] for p in params})
                            print_long_iterator(session, db.iterate_data_formatted(*data))

                        stack.pop()

                except Exception as err:
                    msg('Error', repr(err))
                    stack.pop()
                    
        except EOFError:
            break

        except:
            continue

# --------------------------------------------------------------- #

## Main function that creates the Russtat engine and retrieves / stores data.
def main():

    # if len(sys.argv) == 1:
    #     update_db()
    # else:
    #     update_db(*sys.argv[1:])

    cli()

# --------------------------------------------------------------- #

## Program entry point.
if __name__ == '__main__':
    main()