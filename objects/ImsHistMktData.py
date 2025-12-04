# ------------------------------------------------------------
# filename : ImsHistMktData.py
# descr    : handles historic market data
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

class ImsHistMktData:

    # ------------------------------------------------------------
    # class : ImsHistMktData
    # descr : histmktdata class
    #
    # in    : (hmd_inv_ticker, hmd_start_datetime, hmd_end_datetime, hmd_freq_type, hmd_start_bid_price, hmd_highest_bid_price, hmd_lowest_bid_price, hmd_last_bid_price, hmd_start_ask_price, hmd_highest_ask_price, hmd_lowest_ask_price, hmd_last_ask_price, hmd_first_traded_price, hmd_highest_traded_price, hmd_lowest_traded_price, hmd_last_traded_price, hmd_total_traded_volume )
    # out   : n/a
    # ------------------------------------------------------------

    def __init__(self, hmd_inv_ticker, hmd_start_datetime, hmd_end_datetime, hmd_freq_type, hmd_start_bid_price, hmd_highest_bid_price, hmd_lowest_bid_price, hmd_last_bid_price, hmd_start_ask_price, hmd_highest_ask_price, hmd_lowest_ask_price, hmd_last_ask_price, hmd_first_traded_price, hmd_highest_traded_price, hmd_lowest_traded_price, hmd_last_traded_price, hmd_total_traded_volume):
        
        self.hmd_inv_ticker           = hmd_inv_ticker
        self.hmd_start_datetime       = hmd_start_datetime 
        self.hmd_end_datetime         = hmd_end_datetime 
        self.hmd_freq_type            = hmd_freq_type
        self.hmd_start_bid_price      = hmd_start_bid_price
        self.hmd_highest_bid_price    = hmd_highest_bid_price
        self.hmd_lowest_bid_price     = hmd_lowest_bid_price
        self.hmd_last_bid_price       = hmd_last_bid_price
        self.hmd_start_ask_price      = hmd_start_ask_price
        self.hmd_highest_ask_price    = hmd_highest_ask_price
        self.hmd_lowest_ask_price     = hmd_lowest_ask_price
        self.hmd_last_ask_price       = hmd_last_ask_price
        self.hmd_first_traded_price   = hmd_first_traded_price
        self.hmd_highest_traded_price = hmd_highest_traded_price
        self.hmd_lowest_traded_price  = hmd_lowest_traded_price
        self.hmd_last_traded_price    = hmd_last_traded_price
        self.hmd_total_traded_volume  = hmd_total_traded_volume
        
        
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
    
        agt_db.agt_put(table_name = 'IMS_HIST_MKT_DATA', row_data = [obj_name_val_dict(obj = self)])  
          
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