# -*- coding: utf-8 -*-
# Copyright: (c) 2020, Iskander Shafikov <s00mbre@gmail.com>
# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## @package russtat.globs
# @brief Global variables.
from datetime import datetime, timedelta

# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## `bool` toggle to turn console debug messages on/off
DEBUGGING = False

# --------------------------------------------------------------- #

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
