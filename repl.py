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
import argparse, itertools, os
from rsengine import Russtat
from psdb import Russtatdb, pd
from russtat import add2db
from globs import NL, report

# --------------------------------------------------------------- #

RQUIT = '<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit'

def COLOR_FIRST(txt, col='OrangeRed'):
    return f'<{col}>{txt[0]}</{col}>{txt[1:]}'

REPLMENU = {
    '!': {
        'MENU': f'1 - {COLOR_FIRST("datasets")} 2 - {COLOR_FIRST("observations")} 3 - {COLOR_FIRST("sql query (raw)")} 4 - {COLOR_FIRST("info")} 5 - {COLOR_FIRST("update DB")}\n{COLOR_FIRST("Quit")}',
        'd': {
            'MENU': f'1 - {COLOR_FIRST("display all")} 2 - {COLOR_FIRST("find in text fields")} 3 - display {COLOR_FIRST("column names")} 4 - {COLOR_FIRST("search")} 5 - {COLOR_FIRST("print")} categories{NL}{RQUIT}',
            'd': f'SET OUTPUT PARAMETERS{NL}{RQUIT}',
            'f': f'ENTER SEARCH PHRASE{RQUIT}',
            'c': RQUIT,
            's': {
                'MENU': f'1 - by {COLOR_FIRST("id")} 2 - by {COLOR_FIRST("name")} 3 - by {COLOR_FIRST("category")} 4 - {COLOR_FIRST("extended")} search{NL}{RQUIT}',
                'i': f'ENTER IDS SEPARATING WITH COMMA{NL}{RQUIT}',
                'n': f'ENTER NAME OR PART OF IT{NL}{RQUIT}',
                'c': f'ENTER CATEGORY OR PART OF IT{NL}{RQUIT}',
                'e': {
                    'MENU': f'1 - {COLOR_FIRST("parameters")} 2 - {COLOR_FIRST("raw")}{NL}{RQUIT}',
                    'p': f'ENTER SEARCH PARAMETERS{NL}{RQUIT}',
                    'r': f'ENTER SQL QUERY{NL}{RQUIT}'
                }
            },
            'p': f'SET OUTPUT PARAMETERS{NL}{RQUIT}'
        },
        'o': {
            'MENU': f'1 - {COLOR_FIRST("display all")} 2 - {COLOR_FIRST("find")} in text fields 3 - display {COLOR_FIRST("column")} names 4 - {COLOR_FIRST("search")}{NL}{RQUIT}',
            'd': f'SET OUTPUT PARAMETERS{NL}{RQUIT}',
            'f': f'ENTER SEARCH PHRASE{NL}{RQUIT}',
            'c': RQUIT,
            's': {
                'MENU': f'1 - by {COLOR_FIRST("id")} 2 - by dataset {COLOR_FIRST("name")} 3 - by {COLOR_FIRST("dataset")} id 4 - {COLOR_FIRST("extended")} search{NL}{RQUIT}',
                'i': f'ENTER IDS SEPARATING WITH COMMA{NL}{RQUIT}',
                'n': f'ENTER DATASET NAME OR PART OF IT{NL}{RQUIT}',
                'd': f'ENTER DATASET IDS SEPARATING WITH COMMA{NL}{RQUIT}',
                'e': {
                    'MENU': f'1 - {COLOR_FIRST("parameters")} 2 - {COLOR_FIRST("raw")}{NL}{RQUIT}',
                    'p': f'ENTER SEARCH PARAMETERS{NL}{RQUIT}',
                    'r': f'ENTER SQL QUERY{NL}{RQUIT}'
                }
            }
        },
        's': f'ENTER SQL QUERY{NL}{RQUIT}',
        'i': RQUIT,
        'u': {
            'MENU': f'1 - {COLOR_FIRST("full")} update 2 - {COLOR_FIRST("new")} datasets only{NL}{RQUIT}',
            'f': RQUIT,
            'n': RQUIT
        }
    }
}

MENU_FILESTORE = '<OrangeRed>Store to file: </OrangeRed> <yellow>> filename</yellow> '\
                 '[<yellow>f=</yellow><red>csv</red>|xls|hdf|json] '\
                 '[<yellow>r=</yellow>[start_range]-[end_range], [number], ...]\n<OrangeRed>Return</OrangeRed> = cancel'

# --------------------------------------------------------------- #

def get_menu(stack):
    nd = REPLMENU
    try:
        for m in stack:
            nd = nd[m]
        return (formatted_text.HTML(nd['MENU'] if isinstance(nd, dict) else nd), nd)
    except:
        return (formatted_text.HTML(REPLMENU['!']['MENU']), REPLMENU['!']) 

def print_msg(text):
    print_formatted_text(formatted_text.HTML(text))

def print_err(err):
    print_msg(f'<red>{err}</red>')

def msg(title, text, ok_text='OK'):
    return shortcuts.message_dialog(title=title, text=text, ok_text=ok_text).run()

def input_msg(title, text, ok_text='OK', cancel_text='Cancel', password=False):
    return shortcuts.input_dialog(title=title, text=text, ok_text=ok_text, cancel_text=cancel_text, password=password).run()

def yesno_msg(title, text, yes_text='YES', no_text='NO'):
    return shortcuts.yes_no_dialog(title=title, text=text, yes_text=yes_text, no_text=no_text).run()
    
def update_database(db, dbpassword, rs, datasets):
    try:
        db.disable_triggers()

        res = rs.get_many(datasets, del_xml=True, save2json=None, loadfromjson='auto', on_dataset=add2db, 
                        on_dataset_kwargs={'dbparams': {'password': dbpassword}, 'logfile': 'stdout'}, on_error=print_err)
        # get results
        if res:                
            res = res.get()

            if res:                    
                # print summary
                cnt = len(res)
                nonval = sum(1 for x in res if x is None) 
                print_msg(f"<blue>Processed</blue> <yellow>{cnt}</yellow> <blue>datasets:</blue> <yellow>{cnt - nonval}</yellow> <blue>valid,</blue> <red>{nonval}</red> <blue>non-valid.</blue>")

            else:
                print_err('No datasets processed!')

        else:
            print_err('Error executing operation!')

    finally:
        db.enable_triggers()

def print_long_iterator(session, it, print_func=lambda line: print_formatted_text(formatted_text.HTML(line)), 
                        toolbar='----- Hit <violet>Return</violet> to continue, <violet>Ctrl + C</violet> to abort -----', max_rows=50):
    n = 0
    for line in it:
        try:        
            n += 1
            if print_func: print_func(line)
            if max_rows > 0 and n % max_rows == 0:
                session.prompt('', bottom_toolbar=formatted_text.HTML(toolbar))
        except:
            break

def print_datasets(session, data, db, dbpassword, rs, stack, **kwargs):
    lines = db.iterate_data_formatted(*data, colname_handler=lambda c: f"<yellow>{c}</yellow>", 
                                      filler='<blue>========================================</blue>',
                                      number_handler=lambda i: f"<green>{i + 1}</green>")
    print_long_iterator(session, lines, **kwargs)

    # suggest options (update...)
    if not data or not data[1]: return

    opt = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}]: SEARCH >> "), 
                            auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                            bottom_toolbar=formatted_text.HTML(f'1 - {COLOR_FIRST("update listed datasets")}{NL}{RQUIT}'))
    if opt == 'u':
        # dataset names
        datasets = [e[2] for e in data[1]]
        print(datasets[:10])
        if yesno_msg('Proceed with update?', f'Update {len(datasets)} datasets.'):
            update_database(db, dbpassword, rs, datasets)

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

def output_data(session, data, db, stack):
    if not data or not data[1]: return

    parser = argparse.ArgumentParser(prog='', add_help=False)
    parser.add_argument('file', help='output filename (relative or abs path)')
    parser.add_argument('--format', '-f', dest='fmt', choices=['csv', 'xls', 'hdf', 'json', 'html', 'clip'], default='csv', help='file format')
    parser.add_argument('--rows', '-r', dest='rows', default='1:', help='row selection')

    params = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}] >> "), 
                    auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                    bottom_toolbar=formatted_text.HTML(parser.format_help()))

    if not params: return

    args = parser.parse_args(params.split())
    rng = tuple(int(r.strip()) if r.strip() else None for r in args.rows.split(':'))
    data = (data[0], itertools.islice(data[1], *rng))
    df = db.data_to_dataframe(data)           #.iloc[rng]

    if args.fmt == 'csv':
        df.to_csv(args.file, sep=';', date_format='%Y-%m-%d %H:%M:%S')
    elif args.fmt == 'xls':
        # localize datetimes
        for col in df.columns:                                        
            try: df[col] = df[col].dt.tz_localize(None)
            except: pass                                        
        df.to_excel(args.file, float_format='%.2f')
    elif args.fmt == 'hdf':
        df.to_hdf(args.file, key=os.path.basename(args.file), mode='w')
    elif args.fmt == 'json':
        df.to_json(args.file, force_ascii=False, date_format='iso', indent=2)
    elif args.fmt == 'html':
        df.to_html(args.file, na_rep='')
    elif args.fmt == 'clip':
        df.to_clipboard(date_format='%Y-%m-%d %H:%M:%S')

def cli():    

    stack = ['!']  
            
    dbpassword = input_msg('DB password', '', password=True)

    db = Russtatdb(password=dbpassword)
    if not db:
        msg('Error', 'DB connection failed!')
        return

    rs = Russtat(update_list=True)

    # renderer = output.win32.Win32Output(stdout=sys.stdout, use_complete_width=True)
    # renderer.disable_autowrap()
    # session = PromptSession(output=renderer)
    session = PromptSession()

    text = ''
    while text != 'Q':
        try:
            tb, nd = get_menu(stack)
            text = session.prompt(formatted_text.HTML(f"<OrangeRed>[{' -> '.join(stack)}]</OrangeRed><yellow> >> </yellow>"), 
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
                        data = db.get_datasets(get_header=True) # 2-tuple: (colnames list, Cursor)
                        print_datasets(session, data, db, dbpassword, rs, stack)

                        try:
                            output_data(session, data, db, stack)                            

                        finally:
                            stack.pop()
                                           
                    elif path == '!/d/f':
                        text1 = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}]: SEARCH >> "), 
                                        auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                                        bottom_toolbar=tb)
                        if text1:
                            data = db.findin_datasets(text1, get_header=True)
                            print_datasets(session, data, db, dbpassword, rs, stack)
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
                            print_datasets(session, data, db, dbpassword, rs, stack)
                        stack.pop()

                    elif path == '!/d/s/n':
                        text1 = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}]: (PART OF) NAME (IN ANY CASE) >> "), 
                                        auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                                        bottom_toolbar=tb)
                        if text1:
                            data = db.get_datasets_by_name(text1, get_header=True)
                            print_datasets(session, data, db, dbpassword, rs, stack)
                        stack.pop()

                    elif path == '!/d/s/c':
                        text1 = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}]: (PART OF) CATEGORY (IN ANY CASE) >> "), 
                                        auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                                        bottom_toolbar=tb)
                        if text1:
                            data = db.get_datasets_by_name(text1, 'classifier', get_header=True)
                            print_datasets(session, data, db, dbpassword, rs, stack)
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
                            print_datasets(session, data, db, dbpassword, rs, stack)

                        stack.pop()

                    elif path == '!/d/s/e/r':
                        text1 = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}]: SQL QUERY >> "), 
                                        auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                                        bottom_toolbar=tb)
                        if text1:
                            data = db.fetch(text1, get_header=True)
                            print_datasets(session, data, db, dbpassword, rs, stack)
                        stack.pop()

                    elif path == '!/o/d':
                        data = db.get_data(get_header=True) # 2-tuple: (colnames list, Cursor)
                        print_datasets(session, data, db, dbpassword, rs, stack)

                        try:
                            output_data(session, data, db, stack)
                        finally:
                            stack.pop()

                    elif path == '!/i':
                        # display info
                        lst = db.get_classificator()
                        print(lst)
                        stack.pop()

                    elif path.startswith('!/u/'):
                        #db.update_dataset_list(overwrite=True)
                        start_num = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}]: STARTING NUMBER (HIT RETURN TO SET TO FIRST) >> "), 
                                        auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                                        bottom_toolbar=tb)
                        end_num = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}]: END NUMBER (HIT RETURN TO SET TO LAST) >> "), 
                                        auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                                        bottom_toolbar=tb)
                        datasets = rs[int(start_num) if start_num else 0:int(end_num) if end_num else None]
                        if path.endswith('n'):
                            datasets = rs.filter_datasets(db, datasets, 'new')
                        # print number of available and new datasets
                        try:
                            confmsg = f"{len(datasets)} datasets / {len(rs)} ({int(len(datasets) * 100.0 / len(rs))}%) to add/update. Proceed?"
                        except:
                            confmsg = f'{len(datasets)} datasets to add/update. Proceed?'

                        if yesno_msg('Proceed with update?', confmsg):
                            update_database(db, dbpassword, rs, datasets)
                        
                        stack.pop()

                except Exception as err:
                    #msg('Error', repr(err))
                    report(None)
                    stack.pop()
                    
        except EOFError:
            break

        except:
            continue

# --------------------------------------------------------------- #

## Program entry point.
if __name__ == '__main__':
    cli()