create table if not exists de10.rdkv_stg_terminals ( 
    terminal_id varchar(6), 
    terminal_type varchar(3),
    terminal_city varchar(50),
    terminal_address varchar(200)
);

create table if not exists de10.rdkv_dwh_dim_terminals_hist ( 
    terminal_id varchar(6), 
    terminal_type varchar(3),
    terminal_city varchar(50),
    terminal_address varchar(200),
    effective_from timestamp(0),
    effective_to timestamp(0) default to_timestamp('9999-12-31', 'YYYY-MM-DD'),
    deleted_flg char(1) default 'N'
);

create table if not exists de10.rdkv_dwh_fact_passport_blacklist (
    passport_num varchar(15),
    entry_dt date
);

create table if not exists de10.rdkv_dwh_fact_tracnsactions (
    trans_id varchar(11),
    trans_date timestamp(0), --change for catching for all the fraud types instead type date
    card_num varchar(20),
    oper_type varchar(8),
    amt decimal,
    oper_result varchar(8),
    terminal varchar(6)
);

/* create automatically in main script (function convert_scd1_to_scd2)
create table if not exists de10.rdkv_stg_cards (
    card_num varchar(20),
    account_num varchar(20),
    start_dt timestamp(0)
);

create table if not exists de10.rdkv_dwh_dim_cards_hist (
    card_num varchar(20),
    account_num varchar(20),
    effective_from timestamp(0),
    effective_to timestamp(0) default to_timestamp('9999-12-31', 'YYYY-MM-DD'),
    deleted_flg char(1) default 'N'    
);

create table if not exists de10.rdkv_stg_accounts (
    account_num varchar(20),
    valid_to date,
    client varchar(10),
    start_dt timestamp(0)
);

create table if not exists de10.rdkv_dwh_dim_accounts_hist (
    account_num varchar(20),
    valid_to date,
    client varchar(10),
    effective_from timestamp(0),
    effective_to timestamp(0) default to_timestamp('9999-12-31', 'YYYY-MM-DD'),
    deleted_flg char(1) default 'N'
);

create table if not exists de10.rdkv_stg_clients (
    client_id varchar(10),
    last_name varchar(20),
    first_name varchar(20),
    patronymic varchar(20),
    date_of_birth date,
    passport_num varchar(15),
    passport_valid_to date,
    phone varchar(16),
    start_dt timestamp(0)
);

create table if not exists de10.rdkv_dwh_dim_clients_hist (
    client_id varchar(10),
    last_name varchar(20),
    first_name varchar(20),
    patronymic varchar(20),
    date_of_birth date,
    passport_num varchar(15),
    passport_valid_to date,
    phone varchar(16),
    effective_from timestamp(0),
    effective_to timestamp(0) default to_timestamp('9999-12-31', 'YYYY-MM-DD'),
    deleted_flg char(1) default 'N'
);
*/
create table if not exists de10.rdkv_rep_fraud (
    event_dt timestamp(0), 
    passport varchar(15),
    fio varchar(62),
    phone varchar(16),
    event_type smallint,
    report_dt date
);

-- queue for builing reports
create table if not exists de10.rdkv_stg_rep_fraud_loads (
    load_dt date
);

-- flat temporary table for building reports
create table if not exists de10.rdkv_stg_rep_fraud_tmp (
	trans_id varchar(11),
	event_dt timestamp(0),
	passport varchar(15),
	fio varchar(62),
	phone varchar(16),
	passport_valid_to date,
	acc_valid_to date,
	acc_deleted_flg char(1)
);

create table if not exists de10.rdkv_meta_loads (
    schema_name varchar(30),
    table_name varchar(50),
    update_dt timestamp(0)
);

/*drop all tables
drop table de10.rdkv_stg_accounts;
drop table de10.rdkv_stg_accounts_del;
drop table de10.rdkv_stg_cards;
drop table de10.rdkv_stg_cards_del;
drop table de10.rdkv_stg_clients;
drop table de10.rdkv_stg_clients_del;
drop table de10.rdkv_stg_terminals;
drop table de10.rdkv_dwh_dim_accounts_hist;
drop table de10.rdkv_dwh_dim_cards_hist;
drop table de10.rdkv_dwh_dim_clients_hist;
drop table de10.rdkv_dwh_dim_terminals_hist;
drop table de10.rdkv_dwh_fact_passport_blacklist;
drop table de10.rdkv_dwh_fact_tracnsactions;
drop table de10.rdkv_meta_loads;
drop table de10.rdkv_stg_rep_fraud_loads;
drop table de10.rdkv_stg_rep_fraud_tmp;
drop table de10.rdkv_rep_fraud;
*/