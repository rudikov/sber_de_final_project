-- Insert new rows to target
insert into de10.rdkv_dwh_dim_terminals_hist(terminal_id, terminal_type, terminal_city, terminal_address, effective_from)
select terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    to_timestamp(%s, 'YYYY-MM-DD')
from (
    select terminal_id,
        terminal_type,
        terminal_city,
        terminal_address  
    from de10.rdkv_stg_terminals
    except
    select terminal_id,
        terminal_type,
        terminal_city,
        terminal_address
    from de10.rdkv_dwh_dim_terminals_hist
    where deleted_flg = 'N' and effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
) t;

--insert deleted rows to target
insert into de10.rdkv_dwh_dim_terminals_hist(terminal_id, terminal_type, terminal_city, terminal_address, effective_from, deleted_flg)
select terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    to_timestamp(%s, 'YYYY-MM-DD'), 'Y'
from (
    select t.terminal_id,
        t.terminal_type,
        t.terminal_city,
        t.terminal_address
    from de10.rdkv_dwh_dim_terminals_hist t
    left join de10.rdkv_stg_terminals s on t.terminal_id = s.terminal_id
    where s.terminal_id is null
        and deleted_flg = 'N'
        and effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
) t;  

--Fix effective_to
update de10.rdkv_dwh_dim_terminals_hist
set effective_to = t.effective_from - interval '1 second' 
from de10.rdkv_dwh_dim_terminals_hist t
where rdkv_dwh_dim_terminals_hist.terminal_id = t.terminal_id
    and rdkv_dwh_dim_terminals_hist.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
    and t.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
    and rdkv_dwh_dim_terminals_hist.effective_from < t.effective_from;

/* Fix efficient_to (old version with window functions)
update de10.rdkv_dwh_dim_terminals_hist
set effective_to = tmp.new_effective_to
from (
    select terminal_id,
        effective_from,
        lead(effective_from) over (partition by terminal_id order by effective_from) - interval '1 second' new_effective_to 
    from de10.rdkv_dwh_dim_terminals_hist
    where effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD') 
) tmp
where rdkv_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
    and rdkv_dwh_dim_terminals_hist.effective_from = tmp.effective_from
    and tmp.new_effective_to is not null;
*/

--Update meta 
update de10.rdkv_meta_loads
    set update_dt = to_timestamp(%s, 'YYYY-MM-DD') 
where schema_name = 'de10' and table_name = 'rdkv_stg_terminals';

--Add date for report bulding in the next step
insert into de10.rdkv_stg_rep_fraud_loads(load_dt) values(to_date(%s, 'YYYY-MM-DD'));
