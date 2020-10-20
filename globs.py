# -*- coding: utf-8 -*-
# Copyright: (c) 2020, Iskander Shafikov <s00mbre@gmail.com>
# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## @package russtat.globs
# @brief Global variables.
from datetime import datetime, timedelta
import sys

# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## `bool` toggle to turn console debug messages on/off
DEBUGGING = False
NL = '\n'
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
        }
    }
}

# --------------------------------------------------------------- #

## Prints a message to a file stream / console accounting for globs::DEBUGGING flag.
# @param message `str` message to output
# @param force_print `bool` set to `True` to output message disregarding globs::DEBUGGING
# @param file `file` file stream to output the message to (default = STDOUT)
# @param end `str` message ending suffix (default = new line symbol)
# @param flush `bool` `True` to flush the IO buffer immediately
def report(message, force=False, file=sys.stdout, end='\n', flush=False):
    if force or DEBUGGING:
        print(message, end=end, file=file, flush=flush)

## Checks if an object is iterable (e.g. a collection or iterator).
# @returns `bool` `True` if `obj` is iterable / `False` if not
def is_iterable(obj):
    if isinstance(obj, str): return False
    try:
        _ = iter(obj)
        return True
    except:
        return False

## Timing decorator function.
def timeit(f, printto=None, prefix='>>>> ELAPSED ', suffix=''):
    def wrapped(*args, **kwargs):
        dt1 = datetime.now()
        res = f(*args, **kwargs)
        dif = datetime.now() - dt1
        if printto:
            print(f"{prefix}{str(dif).split('.')[0]}{suffix}", file=printto)
        else:
            print(f"{prefix}{str(dif).split('.')[0]}{suffix}")
        return res
    return wrapped
