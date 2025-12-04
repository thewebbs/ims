# ------------------------------------------------------------
# filename : ImsInvestment.py
# descr    : handles investments
#
# date       ver who change
# ---------- --- --- ------
# 2025-12-04 101 MW  initial write
# ------------------------------------------------------------

from utils.funct_ds import s_obj_get, obj_name_val_dict

# ============================================================================================================================
# config
# ============================================================================================================================

# ============================================================================================================================
# functions
# ============================================================================================================================    

# ============================================================================================================================
# classes
# ============================================================================================================================

class ImsInvestment:

    # ------------------------------------------------------------
    # class : ImsInvestment
    # descr : investment class
    #
    # in    : (inv_ticker, inv_exc_symbol, inv_name, inv_ib_ticker, inv_sec_name, inv_cty_symbol, inv_latest_price_datetime, inv_avg_spread, inv_avg_bid_price, inv_load_priority, inv_load_status, inv_load_prices, inv_load_divs, inv_load_earnings, inv_load_streaks, inv_load_earlyam)
    # out   : n/a
    # ------------------------------------------------------------

    def __init__(self, inv_ticker, inv_exc_symbol, inv_name, inv_ib_ticker, inv_sec_name, inv_cty_symbol, inv_latest_price_datetime, inv_avg_spread, inv_avg_bid_price, inv_load_priority, inv_load_status, inv_load_prices, inv_load_divs, inv_load_earnings, inv_load_streaks, inv_load_earlyam):
        
        self.inv_ticker                = inv_ticker
        self.inv_exc_symbol            = inv_exc_symbol 
        self.inv_name                  = inv_name 
        self.inv_ib_ticker             = inv_ib_ticker
        self.inv_sec_name              = inv_sec_name
        self.inv_cty_symbol            = inv_cty_symbol
        self.inv_latest_price_datetime = inv_latest_price_datetime
        self.inv_avg_spread            = inv_avg_spread
        self.inv_avg_bid_price         = inv_avg_bid_price
        self.inv_load_priority         = inv_load_priority
        self.inv_load_status           = inv_load_status
        self.inv_load_prices           = inv_load_prices
        self.inv_load_divs             = inv_load_divs
        self.inv_load_earnings         = inv_load_earnings
        self.inv_load_streaks          = inv_load_streaks
        self.inv_load_earlyam          = inv_load_earlyam
        
        return
    

    # ========================================================================================================================
    # functions
    # ========================================================================================================================
  
    # --------------------------------------------------------
    # function : obj_get
    # descr    : this returns the class values based on return type either list, tuple or dict
    #
    # in       : (return_as)
    # out      : (result)
    # --------------------------------------------------------
    
    def obj_get(self, return_as = tuple):
        
        result = s_obj_get(obj = self, return_as = return_as)
            
        return result
    
    
    # --------------------------------------------------------
    # function : put_db
    # descr    : this puts the class values to the database
    #
    # in       : (agt_db)
    # out      : ()
    # --------------------------------------------------------
        
    def put_db(self, agt_db):
    
        agt_db.agt_put(table_name = 'IMS_INVESTMENTS', row_data = [obj_name_val_dict(obj = self)])  
          
        return
    
    
    # ========================================================================================================================
    # 
    # ========================================================================================================================

    def flush (self):
        
        pass # to stop runtime error message re no flush
    
        return
    
    
    def __repr__(self): 
        
        return f"{self.exc_symbol}"
    
    
    def __str__(self): 
        
        return f"{self.exc_symbol}"


    def __unicode__(self): 
            
        return f"{self.exc_symbol}"