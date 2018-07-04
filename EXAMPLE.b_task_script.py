# -*- mode: python; coding: utf-8 -*-

import shlex
from lib_pg_perfect_ticker import simple_db_pool

arg_list = shlex.split(ticker_task_ctx.task_script_arg)

assert len(arg_list) == 4

second_con = stack.enter_context(simple_db_pool.get_db_con_ctxmgr(
    ticker_task_ctx.db_pool,
    ticker_task_ctx.db_con_dsn,
))

try:
    with con.cursor() as cur:
        cur.execute('select 345.0 + %(a)s  + %(c)s', {
            'a': arg_list[0],
            'c': arg_list[2],
        })
    
    con.commit()
    
    # and we awakening another task, right now
    awake_task('example_task_a')
finally:
    with second_con.cursor() as cur:
        cur.execute('select \'some value; \' || %(b)s || \' \' || %(d)s', {
            'b': arg_list[1],
            'd': arg_list[3],
        })
    
    second_con.commit()
