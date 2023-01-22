-- Clean abd fill temporary table
delete from de10.rdkv_stg_rep_fraud_tmp;
insert into de10.rdkv_stg_rep_fraud_tmp (
	trans_id,
	event_dt,
	passport,
    fio,
	phone,
	passport_valid_to,
	acc_valid_to,
	acc_deleted_flg)
    select tr.trans_id,
        tr.trans_date event_dt,
        cln.passport_num passport,
        rtrim(concat(cln.last_name, ' ', cln.first_name, ' ', cln.patronymic)) fio,
        cln.phone,
        cln.passport_valid_to,
        acc.valid_to acc_valid_to,
        acc.deleted_flg acc_deleted_flg
    from de10.rdkv_dwh_fact_tracnsactions tr
    inner join de10.rdkv_dwh_dim_cards_hist card on tr.card_num = card.card_num
        and card.deleted_flg = 'N' and tr.trans_date between card.effective_from and card.effective_to 
    inner join de10.rdkv_dwh_dim_accounts_hist acc on card.account_num = acc.account_num
        and tr.trans_date between acc.effective_from and acc.effective_to
    inner join de10.rdkv_dwh_dim_clients_hist cln on acc.client = cln.client_id
        and cln.deleted_flg = 'N' and tr.trans_date between cln.effective_from and cln.effective_to
    where cast(tr.trans_date as date) = %s and tr.oper_result = 'SUCCESS';

-- Add records for previous days
insert into de10.rdkv_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
select event_dt,
    passport,
    fio,
    phone,
    event_type,
    %s 
from de10.rdkv_rep_fraud
where report_dt = cast(event_dt as date);

-- Add data for the 1st fraud type
insert into de10.rdkv_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
select tmp.event_dt,
    tmp.passport,
    tmp.fio,
    tmp.phone,
    1,
    %s 
from de10.rdkv_stg_rep_fraud_tmp tmp 
left join de10.rdkv_dwh_fact_passport_blacklist psp
    on tmp.passport = psp.passport_num and psp.entry_dt <= %s
where psp.passport_num is not null
    or coalesce(tmp.passport_valid_to, %s) < %s;

-- Add data for the 2nd fraud type
insert into de10.rdkv_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
select event_dt,
    passport,
    fio,
    phone,
    2,
    %s 
from de10.rdkv_stg_rep_fraud_tmp tmp 
where coalesce(acc_valid_to, %s) < %s
    or acc_deleted_flg = 'Y';

-- Add data for the 3rd fraud type
insert into de10.rdkv_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
select event_dt,
    passport,
    fio,
    phone,
    3,
    %s 
from de10.rdkv_stg_rep_fraud_tmp tmp 
inner join (
    select t1.trans_id 
    from de10.rdkv_dwh_fact_tracnsactions t1
    inner join de10.rdkv_dwh_fact_tracnsactions t2
        on t1.card_num = t2.card_num
        and extract(epoch from t1.trans_date) - extract(epoch from t2.trans_date) between 0 and 3600
    inner join de10.rdkv_dwh_dim_terminals_hist term on t2.terminal = term.terminal_id
        and term.deleted_flg = 'N' and t2.trans_date between term.effective_from and term.effective_to  
    where cast(t1.trans_date as date) = %s
        and t1.oper_result = 'SUCCESS' and t2.oper_result = 'SUCCESS'
    group by t1.trans_id
    having count(distinct term.terminal_city) > 1
) tp3 on tmp.trans_id = tp3.trans_id;

-- Add data for the 4th fraud type
insert into de10.rdkv_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
select event_dt,
    passport,
    fio,
    phone,
    4,
    %s 
from de10.rdkv_stg_rep_fraud_tmp tmp 
inner join ( -- trans_id with 4th type of fraud
    select trans_id,
        finish_amt
    from (
        select t1.trans_id,
            t1.amt finish_amt,
            t2.oper_result,
            t2.amt,
            t2.trans_date,
            coalesce(lag(t2.amt) over (partition by t1.trans_id order by t2.trans_date), t2.amt + 1) prev_amt,
            row_number() over (partition by t1.trans_id order by t2.trans_date desc) nn,
            count(*) over (partition by t1.trans_id) cnt
        from de10.rdkv_dwh_fact_tracnsactions t1
        inner join de10.rdkv_dwh_fact_tracnsactions t2
            on t1.card_num = t2.card_num
            and extract(epoch from t1.trans_date) - extract(epoch from t2.trans_date) between 1 and 1200
        where cast(t1.trans_date as date) = %s
            and t1.oper_result = 'SUCCESS'
    ) t3
    where cnt >= 3 and nn <= 3
    group by trans_id, finish_amt
    having sum(case when oper_result = 'SUCCESS' or prev_amt - amt <= 0 then 1 else 0 end) = 0
            and min(amt) > finish_amt
) tp4 on tmp.trans_id = tp4.trans_id;

-- Remove current date from the queue
delete from de10.rdkv_stg_rep_fraud_loads where load_dt = %s;

/* OLD VERSION
    select tr.trans_date,
        cln.passport_num,
        rtrim(concat(cln.last_name, ' ', cln.first_name, ' ', cln.patronymic)),
        cln.phone,
        1,
        '2021-03-03'
    from de10.rdkv_dwh_fact_tracnsactions tr
    inner join de10.rdkv_dwh_dim_cards_hist card on tr.card_num = card.card_num
        and card.deleted_flg = 'N' and tr.trans_date between card.effective_from and card.effective_to 
    inner join de10.rdkv_dwh_dim_accounts_hist acc on card.account_num = acc.account_num
        and acc.deleted_flg = 'N' and tr.trans_date between acc.effective_from and acc.effective_to
    inner join de10.rdkv_dwh_dim_clients_hist cln on acc.client = cln.client_id
        and cln.deleted_flg = 'N' and tr.trans_date between cln.effective_from and cln.effective_to
    left join de10.rdkv_dwh_fact_passport_blacklist psp on cln.passport_num = psp.passport_num
        and psp.entry_dt <= '2021-03-03'
    where cast(tr.trans_date as date) = '2021-03-03'
        and tr.oper_result = 'SUCCESS'
        and (psp.passport_num is not null or
            (cln.passport_valid_to is not null and cln.passport_valid_to < '2021-03-03'))

    select tr.trans_date,
        cln.passport_num,
        rtrim(concat(cln.last_name, ' ', cln.first_name, ' ', cln.patronymic)),
        cln.phone,
        2,
        '2021-03-03'
    from de10.rdkv_dwh_fact_tracnsactions tr
    inner join de10.rdkv_dwh_dim_cards_hist card on tr.card_num = card.card_num
        and card.deleted_flg = 'N' and tr.trans_date between card.effective_from and card.effective_to 
    inner join de10.rdkv_dwh_dim_accounts_hist acc on card.account_num = acc.account_num
        and tr.trans_date between acc.effective_from and acc.effective_to
    inner join de10.rdkv_dwh_dim_clients_hist cln on acc.client = cln.client_id
        and cln.deleted_flg = 'N' and tr.trans_date between cln.effective_from and cln.effective_to
    where cast(tr.trans_date as date) = '2021-03-03'
        and tr.oper_result = 'SUCCESS'
        and ((acc.valid_to is not null and acc.valid_to < '2021-03-03')
            or acc.deleted_flg = 'Y')

    select tr.trans_date,
        cln.passport_num,
        rtrim(concat(cln.last_name, ' ', cln.first_name, ' ', cln.patronymic)),
        cln.phone,
        3,
        '2021-03-03'
    from de10.rdkv_dwh_fact_tracnsactions tr
    inner join de10.rdkv_dwh_dim_cards_hist card on tr.card_num = card.card_num
        and card.deleted_flg = 'N' and tr.trans_date between card.effective_from and card.effective_to 
    inner join de10.rdkv_dwh_dim_accounts_hist acc on card.account_num = acc.account_num
        and acc.deleted_flg = 'N' and tr.trans_date between acc.effective_from and acc.effective_to
    inner join de10.rdkv_dwh_dim_clients_hist cln on acc.client = cln.client_id
        and cln.deleted_flg = 'N' and tr.trans_date between cln.effective_from and cln.effective_to
    inner join (
        select t1.trans_id 
        from de10.rdkv_dwh_fact_tracnsactions t1
        inner join de10.rdkv_dwh_fact_tracnsactions t2
            on t1.card_num = t2.card_num
            and extract(epoch from t1.trans_date) - extract(epoch from t2.trans_date) between 0 and 3600
        inner join de10.rdkv_dwh_dim_terminals_hist term on t2.terminal = term.terminal_id
            and term.deleted_flg = 'N' and t2.trans_date between term.effective_from and term.effective_to  
        where cast(t1.trans_date as date) = '2021-03-03'
        group by t1.trans_id
        having count(distinct term.terminal_city) > 1
    ) tp3 on tr.trans_id = tp3.trans_id
*/
