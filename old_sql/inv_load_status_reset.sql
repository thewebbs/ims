-- ------------------------------------------------------------
-- Filename  : inv_load_status_reset.sql
-- Project   : ILS-ava
--
-- Descr     : This holds SQL to produce SQL that updates the
--             inv_load_status column on ims_investments
--
--             Run this as a script in SQLDeveloper. Then right mouse click in the results 
--             window and export as a text files to the desktop.
--             It will take quite some time. Eventually when it's finished, open the file
--             from the desktop
--
--             Edit the file, delete the top line and replace " with nothing. Save and run
--
--             This code is restricted to just TSX (tickers like TO) so need to change
--             for other exchanges.
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
-- 2020-09-04   1 MW  Initial write
-- ...
-- 2021-09-05 100 DW  Added version
-- 2021-09-15 101 DW  Renamed from ims_investments_inv_load_status_reset.sql
-- ------------------------------------------------------------

select 'update ims_investments set inv_load_status = '''||
(select 
  case 
     when round(sum(h3.hmd_last_bid_price)/count(h3.hmd_inv_ticker),2) = 0   then 'No data'
     when round(sum(h3.hmd_last_bid_price)/count(h3.hmd_inv_ticker),2) <= 25 then '<25'
     when round(sum(h3.hmd_last_bid_price)/count(h3.hmd_inv_ticker),2) <=50  then '<50'
     when round(sum(h3.hmd_last_bid_price)/count(h3.hmd_inv_ticker),2) <=75  then '<75'
     when round(sum(h3.hmd_last_bid_price)/count(h3.hmd_inv_ticker),2) <=100 then '<100'
     when round(sum(h3.hmd_last_bid_price)/count(h3.hmd_inv_ticker),2) >100  then 'YPause'
  end case 
from ims_hist_mkt_data h3
where h3.hmd_inv_ticker = inv_ticker)||
''' where inv_ticker = '''||inv_ticker||''';'
from
(select inv_ticker, inv_load_status, inv_avg_bid_price, count(*) as recs_saved
from ims_investments, ims_hist_mkt_data h1
where h1.hmd_inv_ticker (+) = inv_ticker
and inv_load_status != 'Y'
and inv_ticker like '%TO'
group by inv_ticker, inv_load_status, inv_avg_bid_price
);
