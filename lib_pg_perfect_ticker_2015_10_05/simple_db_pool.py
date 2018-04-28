# -*- mode: python; coding: utf-8 -*-

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
