-- ------------------------------------------------------------
-- Filename  : find_tickers_on_NYSE_not_TSX.sql
-- Project   : ILS-ava
--
-- Descr     : This holds SQL to identify tickers that are on NYSE but not on TSX
--             and second piece of SQL that shows the hist mkt data row counts 
-- Params    : None
--
-- Copyright : (c) InvLogik Solutions inc 2020 - 2022
--
-- History   :
--
-- Date       Ver Who Change
-- ---------- --- --- ------
-- 2020-12-28   1 MW  Initial write
-- ...
-- 2021-09-05 100 DW  Added version
-- ------------------------------------------------------------

--
-- find those on NYSE and not TO
--

SELECT i1.inv_ticker FROM   ims_investments i1 WHERE  i1.inv_exc_symbol = 'NYSE' and inv_load_status not like 'Err%' 
and  not exists
(SELECT 1 FROM ims_investments i2 WHERE   i2.inv_exc_symbol = 'TSE' 
AND    substr(i1.inv_ticker,1,instr(i1.inv_ticker,'.')-1)  = substr(i2.inv_ticker,1,instr(i2.inv_ticker,'.')-1)  )
and not exists
(select 1 from ims_load_done where ldo_inv_ticker = inv_ticker)
and not exists
(select 1 from ims_load_todos where lto_inv_ticker = inv_ticker and lto_status in ('162','165','200','RDYHOLD'))
order by 1;


--
-- find those on NYSE and on TO
--

SELECT i1.inv_ticker FROM   ims_investments i1 WHERE  i1.inv_exc_symbol = 'NYSE' and inv_load_status not like 'Err%' 
and  exists
(SELECT 1 FROM ims_investments i2 WHERE   i2.inv_exc_symbol = 'TSE' 
AND    substr(i1.inv_ticker,1,instr(i1.inv_ticker,'.')-1)  = substr(i2.inv_ticker,1,instr(i2.inv_ticker,'.')-1)  )
and not exists
(select 1 from ims_load_done where ldo_inv_ticker = inv_ticker)
and not exists
(select 1 from ims_load_todos where lto_inv_ticker = inv_ticker and lto_status in ('162','165','200','RDYHOLD'))
order by 1
;

