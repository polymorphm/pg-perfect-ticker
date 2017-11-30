# -*- mode: python; coding: utf-8 -*-
#
# Copyright (c) 2015 Andrei Antonov <polymorphm@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

assert str is not bytes

import threading
import psycopg2
import contextlib

class SimpleDbPool:
    def __init__(self):
        self.lock = threading.RLock()
        self.con_list_by_dsn = {}
    
    def new_con(self, dsn):
        return psycopg2.connect(dsn)
    
    def check_con(self, con):
        try:
            con.reset()
        except (psycopg2.Warning, psycopg2.Error):
            return False
        
        return True
    
    def get_con(self, dsn):
        with self.lock:
            try:
                con_list = self.con_list_by_dsn[dsn]
            except KeyError:
                con_list = self.con_list_by_dsn[dsn] = []
            
            try:
                con = con_list.pop()
            except IndexError:
                con = None
        
        if con is None:
            return self.new_con(dsn)
        
        if not self.check_con(con):
            con.close()
            
            return self.new_con(dsn)
        
        return con
    
    def save_con(self, dsn, con):
        try:
            con.reset()
        except (psycopg2.Warning, psycopg2.Error):
            con.close()
            
            return
        
        with self.lock:
            try:
                con_list = self.con_list_by_dsn[dsn]
            except KeyError:
                con_list = self.con_list_by_dsn[dsn] = []
            
            con_list.append(con)
    
    def close(self):
        all_con_list = []
        
        with self.lock:
            for dsn in self.con_list_by_dsn:
                con_list = self.con_list_by_dsn[dsn]
                
                for con in con_list:
                    all_con_list.append(con)
            
            self.con_list_by_dsn.clear()
        
        for con in all_con_list:
            con.close()

@contextlib.contextmanager
def get_db_con_ctxmgr(db_pool, dsn):
    con = db_pool.get_con(dsn)
    
    try:
        yield con
    finally:
        db_pool.save_con(dsn, con)
