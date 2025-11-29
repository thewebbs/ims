-- ------------------------------------------------------------
-- Filename  : find_tickers_on_NYSE_and_TSX.sql
-- Project   : ILS-ava
--
-- Descr     : This holds SQL to identify tickers that are on both TSX and NYSE
--
-- Params    : None
--
-- Copyright : (c) InvLogik Solutions inc 2020 - 2022
--
--
-- History   :
--
-- Date       Ver Who Change
-- ---------- --- --- ------
-- 2020-12-28   1 MW  Initial write
-- ...
-- 2021-09-05 100 DW  Added version
-- ------------------------------------------------------------

SELECT i1.inv_ticker, i2.inv_ticker 
FROM   ims_investments i1, ims_investments i2 
WHERE  i1.inv_exc_symbol = 'NYSE' 
AND    i2.inv_exc_symbol = 'TSE' 
AND    substr(i1.inv_ticker,1,instr(i1.inv_ticker,'.')-1)  = substr(i2.inv_ticker,1,instr(i2.inv_ticker,'.')-1)  
order by 1
;
