# ------------------------------------------------------------
# class : ImsSector
# descr : sector class
#
# in    : (sec_name, sec_rep_order)
# out   : n/a
# ------------------------------------------------------------

from utils.funct_ds import s_obj_get, obj_name_val_dict

# ============================================================================================================================
# config
# ============================================================================================================================

# ============================================================================================================================
# classes
# ============================================================================================================================

class ImsSector:

    def __init__(self, sec_name, sec_rep_order):
        
        self.sec_name      = sec_name
        self.sec_rep_order = sec_rep_order 
        
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
    
        agt_db.agt_put(table_name = 'IMS_SECTORS', row_data = [obj_name_val_dict(obj = self)])  
          
        return
    
    
    # ========================================================================================================================
    # 
    # ========================================================================================================================

    def flush (self):
        
        pass # to stop runtime error message re no flush
    
        return
    
    
    def __repr__(self): 
        
        return f"{self.sec_name}"
    
    
    def __str__(self): 
        
        return f"{self.sec_name}"


    def __unicode__(self): 
            
        return f"{self.sec_name}"