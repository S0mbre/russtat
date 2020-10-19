# -*- coding: utf-8 -*-
# Copyright: (c) 2020, Iskander Shafikov <s00mbre@gmail.com>
# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## @package russtat.globs
# @brief Global variables.
from datetime import datetime as dt, timedelta
import sys

# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## `bool` toggle to turn console debug messages on/off
DEBUGGING = False
NL = '\n'

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
        dt1 = dt.now()
        res = f(*args, **kwargs)
        dif = dt.now() - dt1
        if printto:
            print(f"{prefix}{str(dif).split('.')[0]}{suffix}", file=printto)
        else:
            print(f"{prefix}{str(dif).split('.')[0]}{suffix}")
        return res
    return wrapped