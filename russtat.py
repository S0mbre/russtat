# -*- coding: utf-8 -*-
# --------------------------------------------------------------- #

import os, sys
from rsengine import Russtat
from psdb import Psdb

# --------------------------------------------------------------- #

def main():
    #rs = Russtat(update_list=False)
    #print(len(rs))
    #print(rs.get_one("Тариф, установленный для населения на холодное водоснабжение"))
    #res = rs.get_many(rs[:10], loadfromjson=None, on_error=print)
    #print(len(res.get()))
    
    db = Psdb()
    print(bool(db))
    f = db.fetch('select * from agencies', True)
    print(f'RES = {f}')

# --------------------------------------------------------------- #

if __name__ == '__main__':
    main()