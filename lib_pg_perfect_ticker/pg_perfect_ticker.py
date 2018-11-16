# -*- mode: python; coding: utf-8 -*-

import configparser
import os, os.path
import logging
import json
import psycopg2
import contextlib
import asyncio
import concurrent.futures
import time
import math
import traceback

from . import log
from . import simple_db_pool

CONFIG_SECTION = 'pg-perfect-ticker'

class ConfigError(Exception):
    pass

class ConfigCtx:
    pass

class TickerCtx:
    pass

class TickerTaskCtx:
    pass

def blocking_read_config(config_ctx, config_path_list):
    config = configparser.ConfigParser(
        interpolation=configparser.ExtendedInterpolation(),
    )
    
    for config_path in config_path_list:
        config.read(config_path, encoding='utf-8')
    
    thread_pool_list_str = config.get(CONFIG_SECTION, 'thread_pool_list')
    db_con_list_str = config.get(CONFIG_SECTION, 'db_con_list')
    task_list_str = config.get(CONFIG_SECTION, 'task_list')
    
    config_ctx.thread_pool_list = tuple(thread_pool_list_str.split())
    config_ctx.db_con_list = tuple(db_con_list_str.split())
    config_ctx.task_list = tuple(task_list_str.split())
    
    config_ctx.max_workers_by_thread_pool_map = {}
    config_ctx.dsn_by_db_con_map = {}
    config_ctx.log_dsn_by_db_con_map = {}
    config_ctx.log_sql_by_db_con_map = {}
    config_ctx.disabled_by_task_map = {}
    config_ctx.sql_by_task_map = {}
    config_ctx.script_by_task_map = {}
    config_ctx.script_arg_by_task_map = {}
    config_ctx.script_file_by_task_map = {}
    config_ctx.script_exe_by_task_map = {}
    config_ctx.stmt_timeout_by_task_map = {}
    config_ctx.timer_by_task_map = {}
    config_ctx.thread_pool_by_task_map = {}
    config_ctx.db_con_by_task_map = {}
    
    for thread_pool in config_ctx.thread_pool_list:
        value = config.getint(CONFIG_SECTION, '{}.max_workers'.format(thread_pool))
        
        if value < 1:
            raise ConfigError('invalid value of max_workers param: {!r}'.format(value))
        
        config_ctx.max_workers_by_thread_pool_map[thread_pool] = value
    
    for db_con in config_ctx.db_con_list:
        dsn_value = config.get(CONFIG_SECTION, '{}.dsn'.format(db_con))
        log_dsn_value = config.get(
            CONFIG_SECTION,
            '{}.log_dsn'.format(db_con),
            fallback=None,
        )
        log_sql_value = config.get(
            CONFIG_SECTION,
            '{}.log_sql'.format(db_con),
            fallback=None,
        )
        
        config_ctx.dsn_by_db_con_map[db_con] = dsn_value
        config_ctx.log_dsn_by_db_con_map[db_con] = log_dsn_value
        config_ctx.log_sql_by_db_con_map[db_con] = log_sql_value
    
    for task in config_ctx.task_list:
        disabled_value = config.getboolean(
            CONFIG_SECTION,
            '{}.disabled'.format(task),
            fallback=False,
        )
        sql_value = config.get(
            CONFIG_SECTION,
            '{}.sql'.format(task),
            fallback=None,
        )
        script_value = config.get(
            CONFIG_SECTION,
            '{}.script'.format(task),
            fallback=None,
        )
        script_arg_value = config.get(
            CONFIG_SECTION,
            '{}.script_arg'.format(task),
            fallback=None,
        )
        stmt_timeout_value = config.getfloat(
            CONFIG_SECTION,
            '{}.stmt_timeout'.format(task),
            fallback=None,
        )
        timer_value = config.getfloat(CONFIG_SECTION, '{}.timer'.format(task))
        thread_pool_value = config.get(CONFIG_SECTION, '{}.thread_pool'.format(task))
        db_con_value = config.get(CONFIG_SECTION, '{}.db_con'.format(task))
        
        if \
                (sql_value is not None) + \
                (script_value is not None) != 1:
            raise ConfigError('invalid value of {{sql, script}} params: {{{!r}, {!r}}}'.format(
                sql_value,
                script_value,
            ))
        
        if stmt_timeout_value is not None and stmt_timeout_value <= 0:
            raise ConfigError(
                'invalid value of stmt_timeout param: {!r}'.format(
                    stmt_timeout_value,
                ),
            )
        
        if timer_value <= 0:
            raise ConfigError('invalid value of timer param: {!r}'.format(timer_value))
        
        if thread_pool_value not in config_ctx.thread_pool_list:
            raise ConfigError('invalid value of thread_pool param: {!r}'.format(thread_pool_value))
        
        if db_con_value not in config_ctx.db_con_list:
            raise ConfigError('invalid value of db_con param: {!r}'.format(db_con_value))
        
        if script_value is not None:
            script_file_value = os.path.join(
                os.path.dirname(config_path),
                script_value,
            )
            with open(script_file_value, encoding='utf-8') as source_fd:
                source = source_fd.read()
            
            script_exe_value = compile(source, script_file_value, 'exec')
        else:
            script_file_value = None
            script_exe_value = None
        
        config_ctx.disabled_by_task_map[task] = disabled_value
        config_ctx.sql_by_task_map[task] = sql_value
        config_ctx.script_by_task_map[task] = script_value
        config_ctx.script_arg_by_task_map[task] = script_arg_value
        config_ctx.stmt_timeout_by_task_map[task] = stmt_timeout_value
        config_ctx.timer_by_task_map[task] = timer_value
        config_ctx.thread_pool_by_task_map[task] = thread_pool_value
        config_ctx.db_con_by_task_map[task] = db_con_value
        config_ctx.script_file_by_task_map[task] = script_file_value
        config_ctx.script_exe_by_task_map[task] = script_exe_value

def blocking_ticker_task_process(
            ticker_task_ctx,
            awake_task_func,
            set_exc_tb_func,
        ):
    with contextlib.ExitStack() as stack:
        if ticker_task_ctx.db_con_log_sql is not None:
            if ticker_task_ctx.db_con_log_dsn is not None:
                db_con_log_dsn = ticker_task_ctx.db_con_log_dsn
            else:
                db_con_log_dsn = ticker_task_ctx.db_con_dsn
            
            log_con = stack.enter_context(simple_db_pool.get_db_con_ctxmgr(
                ticker_task_ctx.db_pool,
                db_con_log_dsn
            ))
            
            assert not log_con.autocommit
        else:
            log_con = None
        
        if log_con is not None:
            with log_con.cursor() as log_cur:
                log_cur.execute(ticker_task_ctx.db_con_log_sql, {
                    'log_data': json.dumps({
                        'event': 'ticker_task_execute',
                        'task': ticker_task_ctx.task_name,
                        'thread_pool': ticker_task_ctx.thread_pool_name,
                        'db_con': ticker_task_ctx.db_con_name,
                    })
                })
            
            log_con.commit()
        
        exc_tb = None
        
        try:
            con = stack.enter_context(simple_db_pool.get_db_con_ctxmgr(
                ticker_task_ctx.db_pool,
                ticker_task_ctx.db_con_dsn,
            ))
            
            assert not con.autocommit
            
            if ticker_task_ctx.task_stmt_timeout is not None:
                with con.cursor() as cur:
                    cur.execute(
                        'set statement_timeout to %(statement_timeout)s',
                        {
                            'statement_timeout': math.ceil(
                                ticker_task_ctx.task_stmt_timeout * 1000,
                            ),
                        },
                    )
                
                con.commit()
            
            if ticker_task_ctx.task_sql is not None:
                con.autocommit = True
                
                with con.cursor() as cur:
                    cur.execute(ticker_task_ctx.task_sql)
            elif ticker_task_ctx.task_script_exe is not None:
                try:
                    exec(ticker_task_ctx.task_script_exe, None, {
                        'stack': stack,
                        'ticker_task_ctx': ticker_task_ctx,
                        'con': con,
                        'awake_task': awake_task_func,
                    })
                except Exception as e:
                    exc_tb = traceback.format_exc()
                    
                    set_exc_tb_func(exc_tb)
                    
                    raise e from e
            else:
                raise NotImplementedError
        except Exception as e:
            if log_con is not None:
                with log_con.cursor() as log_cur:
                    log_cur.execute(ticker_task_ctx.db_con_log_sql, {
                        'log_data': json.dumps({
                            'event': 'ticker_task_error',
                            'task': ticker_task_ctx.task_name,
                            'thread_pool': ticker_task_ctx.thread_pool_name,
                            'db_con': ticker_task_ctx.db_con_name,
                            'exc_type': str(type(e)),
                            'exc_str': str(e),
                            'exc_tb': exc_tb,
                        })
                    })
                
                log_con.commit()
            
            raise e from e
        else:
            if log_con is not None:
                with log_con.cursor() as log_cur:
                    log_cur.execute(ticker_task_ctx.db_con_log_sql, {
                        'log_data': json.dumps({
                            'event': 'ticker_task_no_error',
                            'task': ticker_task_ctx.task_name,
                            'thread_pool': ticker_task_ctx.thread_pool_name,
                            'db_con': ticker_task_ctx.db_con_name,
                        })
                    })
                
                log_con.commit()

async def ticker_task_process(loop, ticker_task_ctx, awake_event, awake_event_map):
    try:
        log.log(logging.INFO, 'ticker task ({!r}, {!r}, {!r}): enter'.format(
            ticker_task_ctx.task_name,
            ticker_task_ctx.thread_pool_name,
            ticker_task_ctx.db_con_name,
        ))
        
        exe_start_time = time.monotonic()
        timer = ticker_task_ctx.task_timer
        
        while True:
            log.log(logging.INFO, 'ticker task ({!r}, {!r}, {!r}): execute'.format(
                ticker_task_ctx.task_name,
                ticker_task_ctx.thread_pool_name,
                ticker_task_ctx.db_con_name,
            ))
            
            awake_event.clear()
            
            def awake_task_func(task_name):
                task_awake_event = awake_event_map.get(task_name)
                
                if task_awake_event is None:
                    return False
                
                loop.call_soon_threadsafe(task_awake_event.set)
                
                return True
            
            exc_tb = None
            
            def set_exc_tb_func(tb):
                nonlocal exc_tb
                
                exc_tb = tb
            
            exe_fut = loop.run_in_executor(
                ticker_task_ctx.thread_pool,
                blocking_ticker_task_process,
                ticker_task_ctx,
                awake_task_func,
                set_exc_tb_func,
            )
            
            await asyncio.wait((exe_fut,), loop=loop)
            
            if exe_fut.done() and exe_fut.exception():
                exc_type = type(exe_fut.exception())
                exc_str = str(exe_fut.exception())
                
                if exc_tb is not None:
                    msg_tail = '\n{}'.format(exc_tb.rstrip())
                else:
                    msg_tail = ''
                
                log.log(
                    logging.WARNING,
                    'ticker task ({!r}, {!r}, {!r}): error {!r}: {}{}'.format(
                        ticker_task_ctx.task_name,
                        ticker_task_ctx.thread_pool_name,
                        ticker_task_ctx.db_con_name,
                        exc_type,
                        exc_str,
                        msg_tail,
                    )
                )
            
            exe_stop_time = time.monotonic()
            fixed_timer = timer - (exe_stop_time - exe_start_time)
            
            if fixed_timer < 0:
                fixed_timer = 0
            
            if fixed_timer > timer:
                fixed_timer = timer
            
            exe_start_time = exe_stop_time + fixed_timer
            
            if fixed_timer:
                log.log(logging.INFO, 'ticker task ({!r}, {!r}, {!r}): sleep {!r}'.format(
                    ticker_task_ctx.task_name,
                    ticker_task_ctx.thread_pool_name,
                    ticker_task_ctx.db_con_name,
                    fixed_timer,
                ))
                
                timer_handle = loop.call_later(fixed_timer, awake_event.set)
                
                try:
                    await awake_event.wait()
                finally:
                    timer_handle.cancel()
            else:
                log.log(logging.INFO, 'ticker task ({!r}, {!r}, {!r}): no sleep'.format(
                    ticker_task_ctx.task_name,
                    ticker_task_ctx.thread_pool_name,
                    ticker_task_ctx.db_con_name,
                ))
    finally:
        log.log(logging.INFO, 'ticker task ({!r}, {!r}, {!r}): exit'.format(
            ticker_task_ctx.task_name,
            ticker_task_ctx.thread_pool_name,
            ticker_task_ctx.db_con_name,
        ))

async def ticker_init(loop, ticker_ctx, config_path_list, config_ctx):
    log.log(logging.INFO, 'ticker init: begin')
    
    ticker_ctx.config_path_list = config_path_list
    ticker_ctx.config_ctx = config_ctx
    ticker_ctx.shutdown_event = asyncio.Event(loop=loop)
    ticker_ctx.db_pool = simple_db_pool.SimpleDbPool()
    
    ticker_ctx.thread_pool_by_thread_pool_name = {}
    
    for thread_pool_name in ticker_ctx.config_ctx.thread_pool_list:
        thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=ticker_ctx.config_ctx.max_workers_by_thread_pool_map[thread_pool_name]
        )
        
        ticker_ctx.thread_pool_by_thread_pool_name[thread_pool_name] = thread_pool
    
    log.log(logging.INFO, 'ticker init: done')

async def ticker_shutdown_handler(loop, ticker_ctx):
    ticker_ctx.shutdown_event.set()

async def ticker_process(loop, ticker_ctx):
    awake_event_map = {}
    ticker_task_process_fut_list = []
    
    for task_name in ticker_ctx.config_ctx.task_list:
        if ticker_ctx.config_ctx.disabled_by_task_map[task_name]:
            continue
        
        thread_pool_name = ticker_ctx.config_ctx.thread_pool_by_task_map[task_name]
        db_con_name = ticker_ctx.config_ctx.db_con_by_task_map[task_name]
        ticker_task_ctx = TickerTaskCtx()
        
        ticker_task_ctx.task_name = task_name
        ticker_task_ctx.thread_pool_name = thread_pool_name
        ticker_task_ctx.db_con_name = db_con_name
        ticker_task_ctx.thread_pool_max_workers = ticker_ctx.config_ctx.max_workers_by_thread_pool_map[thread_pool_name]
        ticker_task_ctx.db_con_dsn = ticker_ctx.config_ctx.dsn_by_db_con_map[db_con_name]
        ticker_task_ctx.db_con_log_dsn = ticker_ctx.config_ctx.log_dsn_by_db_con_map[db_con_name]
        ticker_task_ctx.db_con_log_sql = ticker_ctx.config_ctx.log_sql_by_db_con_map[db_con_name]
        ticker_task_ctx.task_sql = ticker_ctx.config_ctx.sql_by_task_map[task_name]
        ticker_task_ctx.task_script = ticker_ctx.config_ctx.script_by_task_map[task_name]
        ticker_task_ctx.task_script_arg = ticker_ctx.config_ctx.script_arg_by_task_map[task_name]
        ticker_task_ctx.task_stmt_timeout = ticker_ctx.config_ctx.stmt_timeout_by_task_map[task_name]
        ticker_task_ctx.task_timer = ticker_ctx.config_ctx.timer_by_task_map[task_name]
        ticker_task_ctx.thread_pool = ticker_ctx.thread_pool_by_thread_pool_name[thread_pool_name]
        ticker_task_ctx.db_pool = ticker_ctx.db_pool
        ticker_task_ctx.task_script_file = ticker_ctx.config_ctx.script_file_by_task_map[task_name]
        ticker_task_ctx.task_script_exe = ticker_ctx.config_ctx.script_exe_by_task_map[task_name]
        
        awake_event = asyncio.Event()
        awake_event_map[task_name] = awake_event
        
        ticker_task_process_fut = loop.create_task(
            ticker_task_process(loop, ticker_task_ctx, awake_event, awake_event_map),
        )
        
        ticker_task_process_fut_list.append(ticker_task_process_fut)
    
    try:
        await ticker_ctx.shutdown_event.wait()
    finally:
        for ticker_task_process_fut in ticker_task_process_fut_list:
            ticker_task_process_fut.cancel()

def blocking_ticker_shutdown(ticker_ctx):
    log.log(logging.INFO, 'ticker shutdown: begin')
    
    for thread_pool_name in ticker_ctx.config_ctx.thread_pool_list:
        ticker_ctx.thread_pool_by_thread_pool_name[thread_pool_name].shutdown()
    
    ticker_ctx.db_pool.close()
    
    log.log(logging.INFO, 'ticker shutdown: done')
