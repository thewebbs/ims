# ------------------------------------------------------------
# filename : ImsExchange.py
# descr    : handles exchanges
#
# date       ver who change
# ---------- --- --- ------
# 2025-12-01 101 DW  initial write
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

class ImsExchange:
    
    # ------------------------------------------------------------
    # class : ImsExchange
    # descr : exchanges class
    #
    # in    : (exc_symbol, exc_name, exc_ib_exchange, exc_ib_primary)
    # out   : n/a
    # ------------------------------------------------------------

    def __init__(self, exc_symbol, exc_name, exc_ib_exchange, exc_ib_primary):
        
        self.exc_symbol      = exc_symbol
        self.exc_name        = exc_name 
        self.exc_ib_exchange = exc_ib_exchange 
        self.exc_ib_primary  = exc_ib_primary
        
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
    
        agt_db.agt_put(table_name = 'IMS_EXCHANGES', row_data = [obj_name_val_dict(obj = self)])  
          
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