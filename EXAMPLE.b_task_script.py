# -*- mode: python; coding: utf-8 -*-

from lib_pg_perfect_ticker_2015_10_05 import simple_db_pool

second_con = stack.enter_context(simple_db_pool.get_db_con_ctxmgr(
    ticker_task_ctx.db_pool,
    ticker_task_ctx.db_con_dsn,
))

try:
    with con.cursor() as cur:
        cur.execute('SELECT 345')
    
    con.commit()
finally:
    with second_con.cursor() as cur:
        cur.execute('SELECT 3456')
    
    second_con.commit()
