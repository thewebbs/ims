-- ------------------------------------------------------------
-- Filename : ims_load_todos_monitor.sql
-- Project  : ILS-ava
--
-- Descr    : This holds SQL to check on the status of the ims_load_todo records
--
-- Params   : None
--
-- Copyright: (c) InvLogik Solutions inc 2020 - 2022
--
-- History  :
--
-- Date       Ver Who Change
-- ---------- --- --- ------
-- 2020-03-14   1 MW  Initial write
-- ...
-- 2021-09-05 100 DW  Added version
-- ------------------------------------------------------------

select lto_req_type, lto_status, count(*) 
from ims_load_todos 
where lto_status = 'RDY'
group by lto_req_type, lto_status
order by lto_status;

select lto_req_type, lto_status, count(*) 
from ims_load_todos 
group by lto_req_type, lto_status
order by lto_req_type, lto_status;

select lto_status, count(*) 
from ims_load_todos
where lto_req_type like 'HIST%' 
group by  lto_status
order by lto_status;


select max(ldo_date_loaded) from ims_load_done;