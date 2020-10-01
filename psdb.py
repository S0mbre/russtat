# -*- coding: utf-8 -*-
# --------------------------------------------------------------- #

import psycopg2
from psycopg2 import DatabaseError
from globs import *

# --------------------------------------------------------------- # 

class Psdb:

    def __init__(self, dbname='russtat', user='postgres', password=None, host='127.0.0.1', port='5432'):
        self.con = None        
        self._connparams = None
        self.connect(dbname=dbname, user=user, password=password, host=host, port=port)

    def __del__(self):
        self.disconnect()

    def connect(self, reconnect=False, dbname='russtat', user='postgres', password=None, host='127.0.0.1', port='5432'):
        if not self.con is None and not reconnect:
            return True
        if password is None:
            password = input('>> Enter password:')
        try:
            self.disconnect()
            self.con = psycopg2.connect(database=dbname, user=user, password=password, host=host, port=port)
            self._connparams = (dbname, user, password, host, port)
            if DEBUGGING: print(f'Connected to {self._connparams[0]} as {self._connparams[1]} at {self._connparams[3]}:{self._connparams[4]}')
            return True
        except Exception as err:
            self.con = None
            print(err)
        return False

    def disconnect(self):
        if self.con is None: return True
        try:
            self.con.commit()
            self.con.close()
            self.con = None
            if DEBUGGING: print(f'Disconnected from {self._connparams[0] if self._connparams else "DB"}')
            return True
        except:
            pass
        return False

    def exec(self, sql, exec_params=None, commit=False):
        params = [False] + list(self._connparams) if self._connparams else [False, 'russtat', 'postgres', None, '127.0.0.1', '5432']
        if not self.connect(*params):
            return None
        cur = self.con.cursor()
        try:
            if exec_params:
                cur.execute(sql, exec_params)
            else:
                cur.execute(sql)
            if commit:
                cur.commit()
            return cur
        except Exception as err:
            print(err)
            return None

    def fetch(self, sql, fetch='iter'):      
        cur = self.exec(sql)
        if cur is None: return None
        if fetch == 'list':
            return cur.fetchall()
        elif fetch == 'one':
            return cur.fetchone()
        else:
            return cur

    def __bool__(self):
        return not self.con is None

    def __call__(self):
        return None if self.con is None else self.con.cursor()