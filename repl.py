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

# --------------------------------------------------------------- #

REPLMENU = {
    '!': {
        'MENU': '1 - <OrangeRed>d</OrangeRed>atasets 2 - <OrangeRed>o</OrangeRed>bservations 3 - <OrangeRed>s</OrangeRed>ql query (raw) 4 - <OrangeRed>i</OrangeRed>nfo\n<OrangeRed>Q</OrangeRed>uit',
        'd': {
            'MENU': '1 - <OrangeRed>d</OrangeRed>isplay all 2 - <OrangeRed>f</OrangeRed>ind in text fields 3 - display <OrangeRed>c</OrangeRed>olumn names 4 - <OrangeRed>s</OrangeRed>earch 5 - <OrangeRed>p</OrangeRed>rint categories\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
            'd': 'SET OUTPUT PARAMETERS\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
            'f': 'ENTER SEARCH PHRASE\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
            'c': '<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
            's': {
                'MENU': '1 - by <OrangeRed>i</OrangeRed>d 2 - by <OrangeRed>n</OrangeRed>ame 3 - by <OrangeRed>c</OrangeRed>ategory 4 - <OrangeRed>e</OrangeRed>xtended search\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
                'i': 'ENTER IDS SEPARATING WITH COMMA\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
                'n': 'ENTER NAME OR PART OF IT\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
                'c': 'ENTER CATEGORY OR PART OF IT\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
                'e': {
                    'MENU': '1 - <OrangeRed>p</OrangeRed>arameters 2 - <OrangeRed>r</OrangeRed>aw\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
                    'p': 'ENTER SEARCH PARAMETERS\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
                    'r': 'ENTER SQL QUERY\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit'
                }
            },
            'p': 'SET OUTPUT PARAMETERS\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit'
        },
        'o': {
            'MENU': '1 - <OrangeRed>d</OrangeRed>isplay all 2 - <OrangeRed>f</OrangeRed>ind in text fields 3 - display <OrangeRed>c</OrangeRed>olumn names 4 - <OrangeRed>s</OrangeRed>earch\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
            'd': 'SET OUTPUT PARAMETERS\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
            'f': 'ENTER SEARCH PHRASE\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
            'c': '<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
            's': {
                'MENU': '1 - by <OrangeRed>i</OrangeRed>d 2 - by dataset <OrangeRed>n</OrangeRed>ame 3 - by <OrangeRed>d</OrangeRed>ataset id 4 - <OrangeRed>e</OrangeRed>xtended search\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
                'i': 'ENTER IDS SEPARATING WITH COMMA\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
                'n': 'ENTER DATASET NAME OR PART OF IT\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
                'd': 'ENTER DATASET IDS SEPARATING WITH COMMA\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
                'e': {
                    'MENU': '1 - <OrangeRed>p</OrangeRed>arameters 2 - <OrangeRed>r</OrangeRed>aw\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
                    'p': 'ENTER SEARCH PARAMETERS\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit',
                    'r': 'ENTER SQL QUERY\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit'
                }
            }
        },
        's': 'ENTER SQL QUERY\n<OrangeRed>*</OrangeRed> - RETURN\n<OrangeRed>Q</OrangeRed>uit'
    }
}

MENU_FILESTORE = '<OrangeRed>Store to file: </OrangeRed> <yellow>> filename</yellow> '\
                 '[<yellow>f=</yellow><red>csv</red>|xls|hdf|json] '\
                 '[<yellow>r=</yellow>[start_range]-[end_range], [number], ...]\n<OrangeRed>Return</OrangeRed> = cancel'

# --------------------------------------------------------------- #

def msg(title, text, ok_text='OK'):
    return shortcuts.message_dialog(title=title, text=text, ok_text=ok_text).run()

def input_msg(title, text, ok_text='OK', cancel_text='Cancel', password=False):
    return shortcuts.input_dialog(title=title, text=text, ok_text=ok_text, cancel_text=cancel_text, password=password).run()

def yesno_msg(title, text, yes_text='YES', no_text='NO'):
    return shortcuts.yes_no_dialog(title=title, text=text, yes_text=yes_text, no_text=no_text).run()

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

def print_datasets(session, data, db, **kwargs):
    lines = db.iterate_data_formatted(*data, colname_handler=lambda c: f"<yellow>{c}</yellow>", 
                                      filler='<blue>========================================</blue>',
                                      number_handler=lambda i: f"<green>{i + 1}</green>")
    print_long_iterator(session, lines, **kwargs)

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
                        print_datasets(session, data, db)

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
                            print_datasets(session, data, db)
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
                            print_datasets(session, data, db)
                        stack.pop()

                    elif path == '!/d/s/n':
                        text1 = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}]: (PART OF) NAME (IN ANY CASE) >> "), 
                                        auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                                        bottom_toolbar=tb)
                        if text1:
                            data = db.get_datasets_by_name(text1, get_header=True)
                            print_datasets(session, data, db)
                        stack.pop()

                    elif path == '!/d/s/c':
                        text1 = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}]: (PART OF) CATEGORY (IN ANY CASE) >> "), 
                                        auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                                        bottom_toolbar=tb)
                        if text1:
                            data = db.get_datasets_by_name(text1, 'classifier', get_header=True)
                            print_datasets(session, data, db)
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
                            print_datasets(session, data, db)

                        stack.pop()

                    elif path == '!/d/s/e/r':
                        text1 = session.prompt(formatted_text.HTML(f"[{' -> '.join(stack)}]: SQL QUERY >> "), 
                                        auto_suggest=auto_suggest.AutoSuggestFromHistory(), 
                                        bottom_toolbar=tb)
                        if text1:
                            data = db.fetch(text1, get_header=True)
                            print_datasets(session, data, db)
                        stack.pop()

                    elif path == '!/o/d':
                        data = db.get_data(get_header=True) # 2-tuple: (colnames list, Cursor)
                        print_datasets(session, data, db)

                        try:
                            output_data(session, data, db, stack)
                        finally:
                            stack.pop()

                except Exception as err:
                    msg('Error', repr(err))
                    stack.pop()
                    
        except EOFError:
            break

        except:
            continue

# --------------------------------------------------------------- #

## Program entry point.
if __name__ == '__main__':
    cli()