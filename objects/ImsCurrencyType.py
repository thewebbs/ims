# ------------------------------------------------------------
# class : ImsCurrencyType
# descr : currency type class
#
# in    : (cty_symbol, cty_name)
# out   : n/a
# ------------------------------------------------------------

from utils.funct_ds import s_obj_get, obj_name_val_dict

# ============================================================================================================================
# config
# ============================================================================================================================

# ============================================================================================================================
# classes
# ============================================================================================================================

class ImsCurrencyType:

    def __init__(self, cty_symbol, cty_name):
        
        self.cty_symbol = cty_symbol
        self.cty_name   = cty_name 
        
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
    
        agt_db.agt_put(table_name = 'IMS_CURRENCY_TYPES', row_data = [obj_name_val_dict(obj = self)])  
          
        return
    
    
    # ========================================================================================================================
    # 
    # ========================================================================================================================

    def flush (self):
        
        pass # to stop runtime error message re no flush
    
        return
    
    
    def __repr__(self): 
        
        return f"{self.cty_symbol}"
    
    
    def __str__(self): 
        
        return f"{self.cty_symbol}"


    def __unicode__(self): 
            
        return f"{self.cty_symbol}"