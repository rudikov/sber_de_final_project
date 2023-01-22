#!/usr/bin/python3

import argparse
import datetime
import json
import logging
import os
import zipfile
from pathlib import Path

import pandas as pd

import psycopg2
import psycopg2.extras


def ddl_init(ddl_path: Path, conn):
    with conn.cursor() as cursor, open(ddl_path) as f:
        cursor.execute(f.read())
        conn.commit()
        logging.info(f'Create tables from ddl-init script "{ddl_path}" '
                     'if it is necessary')
        query = ''' insert into de10.rdkv_meta_loads(schema_name, table_name, update_dt)
                select 'de10', 'rdkv_stg_terminals',
                        to_timestamp('1900-01-01','YYYY-MM-DD')
                where not exists (
                select * from de10.rdkv_meta_loads
                where schema_name = 'de10'
                    and table_name = 'rdkv_stg_terminals')
                '''
        cursor.execute(query)
        conn.commit()
        logging.info('Initialize metadata if it is necessary')


def get_update_dt_from_meta(schema_name, table_name, conn):
    query = '''
    select to_char(update_dt, 'YYYY-MM-DD')
    from de10.rdkv_meta_loads
    where schema_name = %s and table_name = %s
    '''
    with conn.cursor() as cursor:
        cursor.execute(query, (schema_name, table_name))
        return cursor.fetchone()[0]


def load_transactions_file(path: Path, conn):
    logging.info(f'Start loading rows from "{path}"')
    # Load transactions from file and parse timestamp and numeric values
    df = pd.read_csv(path, sep=';', decimal=',',
                     parse_dates=['transaction_date'],
                     date_parser=lambda x:
                     datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    query = '''
    insert into de10.rdkv_dwh_fact_tracnsactions
    (trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal)
    values(%s, %s, %s, %s, %s, %s, %s)
    '''
    # Load transactions to the database
    with conn.cursor() as cursor:
        psycopg2.extras.execute_batch(cursor, query, df.values.tolist())
    logging.info(f'End loading rows from "{path}". Loaded {df.shape[0]} rows')


def load_passport_blacklist_file(path: Path, key: str, conn):
    logging.info(f'Start loading rows from "{path}"')
    date = datetime.datetime.strptime(key, '%Y-%m-%d')
    df = pd.read_excel(path)
    shape = df.shape[0]
    # Filter rows per date and calculate amount of the skipped rows
    df = df[df.date == date]
    skipped = shape - df.shape[0]
    # Load the list of passports in the database
    query = ('insert into de10.rdkv_dwh_fact_passport_blacklist'
             '(entry_dt, passport_num) values(%s, %s)')
    with conn.cursor() as cursor:
        psycopg2.extras.execute_batch(cursor, query, df.values.tolist())
    logging.info(f'End loading rows from "{path}". Loaded {df.shape[0]} rows' +
                 (f', skipped {skipped} rows ("date" <> {key})'
                  if skipped != 0 else ''))


def load_terminals_file(path: Path, conn):
    logging.info(f'Start loading rows from "{path}"')
    df = pd.read_excel(path)
    query = ('insert into de10.rdkv_stg_terminals'
             '(terminal_id, terminal_type, terminal_city, terminal_address) '
             'values(%s, %s, %s, %s)')
    with conn.cursor() as cursor:
        cursor.execute('delete from de10.rdkv_stg_terminals')
        psycopg2.extras.execute_batch(cursor, query, df.values.tolist())


def replicate_inline_value(value, query):
    """Helper function to replicate '%s' parameter for all the injections"""
    cnt = len(query) - len(query.replace('%s', 's'))
    return tuple(value for _ in range(cnt))


def convert_terminals_to_scd2(path: Path, dt, conn):
    with conn.cursor() as cursor, open(path) as f:
        # Convert loaded terminals data to SCD2 format
        query = f.read()
        cursor.execute(query, replicate_inline_value(dt, query))
    conn.commit()


def convert_scd1_to_scd2(table, id, renamed_columns, conn_source,
                         conn_target, now, schema_source='info',
                         schema_target='de10'):
    """
    Converting datatable from SCD1 to SCD2 format for two different datasources

    table - table name in source database (str)
    id - table primary key in source database (str)
    renamed_columns - dictionary with ranamed columns,
        key - column name in source,
        value - colum name in target,
    conn_source, conn_target - connection object for source (target) database,
    schema_source, schema_target - schema for source (target) database
    """
    def trim_sql(names):
        """Add trim command to sql substring for character columns"""
        result = []
        for x in names:
            if columns[x].find('char') >= 0:
                result.append(f'rtrim({x})')
            else:
                result.append(x)
        return result

    if renamed_columns is None:
        renamed_columns = dict()
    with conn_source.cursor() as cursor:
        query = f'''select column_name, data_type, character_maximum_length
        from information_schema.columns
        where table_schema = '{schema_source}' and table_name = %s
        '''
        # Fetch columns' names and types from metadata (source)
        cursor.execute(query, (table, ))
        # Create and fill dictionary (name: type)
        columns = dict()
        for x in cursor.fetchall():
            # Skip SCD1 specific namss
            if x[0] not in ('create_dt', 'update_dt'):
                # Replace character types to varchar
                if x[1].split()[0] != 'character':
                    columns[x[0]] = x[1].split()[0]
                else:
                    columns[x[0]] = 'varchar'
                if x[2] is not None:
                    columns[x[0]] += f'({x[2]})'
        # Construct subquery string with names and types for creating table
        create_table_columns_str = '\n,'.join(f'''{renamed_columns.get(k, k)}
            {v}''' for k, v in columns.items())
        # Sort columns' names in source and set the same order in target
        columns_source = list(sorted(columns.keys()))
        columns_target = [renamed_columns.get(k, k) for k in columns_source]
        id_target = renamed_columns.get(id, id)
    with conn_target.cursor() as cursor:
        # Greate stg and hist tables and add initial data to meta
        query = f'''create table if not exists
    {schema_target}.rdkv_stg_{table}(
    {create_table_columns_str} ,
    start_dt timestamp(0) );

    create table if not exists {schema_target}.rdkv_dwh_dim_{table}_hist (
    {create_table_columns_str},
    effective_from timestamp(0),
    effective_to timestamp(0) default to_timestamp('9999-12-31', 'YYYY-MM-DD'),
    deleted_flg char(1) default 'N' );

    create table if not exists {schema_target}.rdkv_stg_{table}_del (
        {id_target} {columns[id]} );

    insert into {schema_target}.rdkv_meta_loads
    (schema_name, table_name, update_dt)
    select '{schema_source}', '{table}', null
    where not exists (
    select * from de10.rdkv_meta_loads
    where schema_name = '{schema_source}' and table_name = '{table}' );
    '''
        cursor.execute(query)
        conn_target.commit()
        # Fetch last update timestamp value for incremental loading
        cursor.execute(f'''select update_dt from de10.rdkv_meta_loads
          where schema_name = '{schema_source}' and table_name = '{table}' ''')
        update_db = cursor.fetchone()[0]
    # Load data from source to temp variable
    query_stg = f'''select {", ".join(trim_sql(columns_source))},
        coalesce(update_dt, create_dt) start_dt
        from {schema_source}.{table}'''
    with conn_source.cursor() as cursor:
        # add incremental condition if it necessary
        if update_db is None:
            cursor.execute(query_stg)
        else:
            query_stg = f'{query_stg} where coalesce(update_dt, create_dt)> %s'
        cursor.execute(query_stg, (update_db, ))
        rows_stg = cursor.fetchall()
        # Load pk table from source to check deleted items
        cursor.execute(f'''select {trim_sql([id])[0]}
                       from {schema_source}.{table}''')
        rows_stg_del = cursor.fetchall()
    with conn_target.cursor() as cursor:
        # Clean stg table in target and load new data
        cursor.execute(f'delete from {schema_target}.rdkv_stg_{table}')
        query = f'''insert into {schema_target}.rdkv_stg_{table}
          ({", ".join(columns_target)}, start_dt )
          values({', '.join('%s' for _ in range(len(columns_target) + 1))})'''
        psycopg2.extras.execute_batch(cursor, query, rows_stg)
        cursor.execute(f'delete from {schema_target}.rdkv_stg_{table}_del')
        query = f'''insert into {schema_target}.rdkv_stg_{table}_del
                ({id_target}) values(%s)'''
        psycopg2.extras.execute_batch(cursor, query, rows_stg_del)
        # Constract join condition for loading to hist
        join_condition = ' and '.join(f'''((s.{x} = t.{x}) or (s.{x} is null
            and t.{x} is null))''' for x in columns_target)
        # Insert data to hist (new or updated rows)
        query = f'''insert into {schema_target}.rdkv_dwh_dim_{table}_hist
            ({", ".join(columns_target)}, effective_from)
            select {', '.join('s.' + x for x in columns_target)}, s.start_dt
            from {schema_target}.rdkv_stg_{table} s
            left join {schema_target}.rdkv_dwh_dim_{table}_hist t
                on {join_condition}
                and t.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
                and t.deleted_flg = 'N'
            where t.{id_target} is null
            '''
        cursor.execute(query)
        # Insert data to hist (deleted rows in source)
        query = f'''insert into {schema_target}.rdkv_dwh_dim_{table}_hist(
            {", ".join(columns_target)}, effective_from, deleted_flg)
            select {", ".join("t." + x for x in columns_target)}, %s, 'Y'
            from {schema_target}.rdkv_dwh_dim_{table}_hist t
            left join {schema_target}.rdkv_stg_{table}_del d
                on t.{id_target} = d.{id_target}
            where d.{id_target} is null
            and t.deleted_flg = 'N' and
            t.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
            '''
        # fix efficient_to attribute for updated rows in hist and update meta
        cursor.execute(query, (now, ))
        query = f'''update {schema_target}.rdkv_dwh_dim_{table}_hist
        set effective_to = t.effective_from - interval '1 second'
        from {schema_target}.rdkv_dwh_dim_{table}_hist t
        where rdkv_dwh_dim_{table}_hist.{id_target} = t.{id_target}
        and rdkv_dwh_dim_{table}_hist.effective_to =
            to_timestamp('9999-12-31', 'YYYY-MM-DD')
        and t.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
        and rdkv_dwh_dim_{table}_hist.effective_from < t.effective_from;

        update {schema_target}.rdkv_meta_loads
        set update_dt = coalesce((select max(start_dt)
        from {schema_target}.rdkv_stg_{table}), (
        select update_dt from {schema_target}.rdkv_meta_loads
        where schema_name='{schema_source}' and table_name='{table}'))
        where schema_name='{schema_source}' and table_name='{table}';
        '''
        cursor.execute(query)
        # fix transacrion
        conn_target.commit()
        logging.info((f'Loading from {schema_source}.{table} '
                     'to DWH is completed'))


def backup_files(in_path: Path, out_path: Path, files: list):
    """Compress and move 'files' from 'in_path' dir to 'out_path'"""
    for f in files:
        p_from: Path = in_path / f
        p_to = out_path / f'{f}.backup.zip'
        with zipfile.ZipFile(p_to, 'w',
                             compression=zipfile.ZIP_DEFLATED,
                             compresslevel=9) as zip_file:
            zip_file.write(p_from, f)
        p_from.unlink()


def build_report(script: Path, conn):
    with conn.cursor() as cursor, open(script) as f:
        # Load report script from file (sql_scripts/rep.sql)
        query = f.read()
        # Fetch list of dates for report building
        cursor.execute('select load_dt from de10.rdkv_stg_rep_fraud_loads')
        dates = tuple(sorted(x[0] for x in cursor.fetchall()))
        # Iterate for dates and build report for every date
        for date in dates:
            params = replicate_inline_value(date, query)
            cursor.execute(query, params)
            conn.commit()
            logging.info(f'Report for {date.strftime("%Y-%m-%d")} is created')


def load_datafiles(in_path: Path, out_path: Path, conn_edu):
    prefixes = ('transactions', 'passport_blacklist', 'terminals')
    # dictionary for storing correct files
    dic = {key: dict() for key in prefixes}
    # Iterate for all files in in_path directory
    for filename in sorted(os.listdir(in_path)):
        is_wrong_file = True
        for prefix in prefixes:
            # Check prefix and suffix in file and its type
            if filename.startswith(prefix) \
                and ((prefix == 'transactions'
                      and filename.endswith('.txt')) or
                     (prefix != 'transactions'
                      and filename.endswith('.xlsx'))) \
                    and Path(in_path / filename).is_file():
                # extract middle part from filename
                dt = filename[len(prefix) + 1: -4
                              if prefix == 'transactions' else -5]
                if len(dt) == 8 and dt.isdigit():
                    dt = f'{dt[-4:]}-{dt[2:4]}-{dt[:2]}'
                    # try to cast middle part as date
                    try:
                        datetime.datetime.strptime(dt, '%Y-%m-%d')
                    except Exception:
                        pass
                    else:
                        dic[prefix][dt] = filename
                        is_wrong_file = False
                        break
        if is_wrong_file:
            logging.info((f'File "{filename}" has wrong filename '
                          'pattern and will not be loaded'))
    # create list of dates for all types of datafiles with right names
    keys = list(sorted(set(x for d in dic.values() for x in d.keys())))
    # chech full set of files per day
    keys_check = [all(len(dic[k].get(key, '')) > 0
                  for k in prefixes) for key in keys]
    # dates with full set of file
    loaded_keys = []
    # Informe about lack of files (if there're less than 3 files per day)
    for i in range(len(keys_check)):
        if not keys_check[i]:
            mess = f'Files for {keys[i]} will not be loaded. Need: '
            dt = f'{keys[i][-2:]}{keys[i][5:7]}{keys[i][:4]}'
            mess += f'transactions_{dt}.txt ' \
                if dic['transactions'].get(keys[i], '') == '' else ''
            mess += f'passport_blacklist_{dt}.xlsx ' \
                if dic['passport_blacklist'].get(keys[i], '') == '' else ''
            mess += f'terminals_{dt}.xlsx ' \
                if dic['terminals'].get(keys[i], '') == '' else ''
            if i + 1 != len(keys_check):
                mess += (f'Files for {", ".join(keys[i + 1:])} '
                         'also will not be loaded')
            logging.warning(mess)
            break
        loaded_keys.append(keys[i])
    # Check already loaded dates in target database
    update_dt = get_update_dt_from_meta('de10', 'rdkv_stg_terminals', conn_edu)
    if len(loaded_keys) == 0:
        logging.warning('There are not files for loading')
        return
    for i in range(len(loaded_keys)):
        if loaded_keys[i] > update_dt:
            if i > 0:
                msg = (f'Files for {", ".join(loaded_keys[:i])} will not '
                       'be loaded because the database already has '
                       f'information for {update_dt}.')
                logging.warning(msg)
                loaded_keys = loaded_keys[i:]
            break
    else:
        msg = (f'Files for {", ".join(loaded_keys)} will not '
               'be loaded because the database already has '
               f'information for {update_dt}.')
        logging.warning(msg)
        loaded_keys = []
    # Loading data ftom files to target database per day
    for key in loaded_keys:
        load_transactions_file(in_path / dic['transactions'][key], conn_edu)
        path = in_path / dic['passport_blacklist'][key]
        load_passport_blacklist_file(path, key, conn_edu)
        load_terminals_file(in_path / dic['terminals'][key], conn_edu)
        path = default_path / 'sql_scripts' / 'terminals_to_scd2.sql'
        convert_terminals_to_scd2(path, key, conn_edu)
        logging.info(f'Files for {key} are successfully loaded')
        backup_files(in_path, out_path, [dic[pref][key] for pref in prefixes])


if __name__ == "__main__":
    try:
        # Load and parse command line arguments
        caption = 'Result DE10 Project, Egor Rudikov'
        parser = argparse.ArgumentParser(description=caption)
        default_path = Path(__file__).resolve().parent
        hint = ('Input directory with files for loading, '
                'default: basedir of main.py')
        parser.add_argument('--indir', type=str, help=hint)
        hint = ('Path to the backup directory, default: '
                'subdirectory archive in basedir of main.py')
        parser.add_argument('--outdir', type=str, help=hint)
        hint = ('Path to the file with databases connections '
                'configuration, default: py_scripts/defailt_dbconf.json')
        parser.add_argument('--dbconf', type=str, help=hint)
        hint = 'Set logging level (info, warning, error), default: info'
        parser.add_argument('--log', type=str, help=hint)
        args = parser.parse_args()
        # Set default values for command line arguments
        log_level = logging.INFO
        if args.log is not None:
            if args.log.lower() == 'warning':
                log_level = logging.WARNING
            elif args.log.lower() == 'error':
                log_level = logging.ERROR
        logging.basicConfig(format='%(asctime)s: %(levelname)s - %(message)s',
                            level=log_level, datefmt='%Y-%m-%d %H:%M:%S')
        logging.info('Start working...')
        indir = default_path \
            if args.indir is None else Path(args.indir).resolve()
        logging.info(f'Set "{indir}" as an input directory')
        outdir = default_path / 'archive' \
            if args.indir is None else Path(args.outdir).resolve()
        logging.info(f'Set "{outdir}" as a backup directory')
        db_conf_path = default_path / 'py_scripts/default_dbconf.json' \
            if args.dbconf is None else Path(args.dbconf).resolve()
        logging.info(f'Set "{db_conf_path}" as a DB configuration file')
        with open(db_conf_path) as f:
            json_db_congig = f.read()
        db_conf = json.loads(json_db_congig)
        # Connect to target database ("edu")
        with psycopg2.connect(**db_conf['target']) as conn_edu:
            msg = 'Connection to "target" database is successfully opened'
            logging.info(msg)
            conn_edu.autocommit = False
            # Initialize target database
            ddl_init(default_path / 'main.ddl', conn_edu)
            with conn_edu.cursor() as cursor:
                # Fix now variable
                cursor.execute('select cast(now() as timestamp(0))')
                now = cursor.fetchone()[0]
            # Connect to source database ("bank")
            with psycopg2.connect(**db_conf['source']) as conn_bank:
                msg = 'Connection to "source" database is successfully opened'
                logging.info(msg)
                conn_bank.autocommit = True
                # Grab data from source and converting to SCD2 format in target
                convert_scd1_to_scd2('accounts', 'account',
                                     {'account': 'account_num'},
                                     conn_bank, conn_edu, now)
                convert_scd1_to_scd2('cards', 'card_num',
                                     {'account': 'account_num'},
                                     conn_bank, conn_edu, now)
                convert_scd1_to_scd2('clients', 'client_id', None,
                                     conn_bank, conn_edu, now)
            # datafiles processing
            load_datafiles(indir, outdir, conn_edu)
            # report processing
            build_report(default_path / 'sql_scripts' / 'rep.sql', conn_edu)
            logging.info('Finish working...')
    except Exception as ex:
        print(ex)
        logging.error('Exception occurred', exc_info=True)
