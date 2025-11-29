#------------------------------------------------------------
# Filename  : AgentMonitor.py
# Project   : ava
#
# Descr     : This holds routines relating to the Monitor Agent class and methods
#   
# Params    : None
#         
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-09-05   1 DW  Initial write
# ...
# 2021-12-19 100 DW  Added version 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------


#
# Class definition
#

class AgentMonitor:
    
    def __init__(self, am_uid, am_contract):
        
        self.am_uid              = am_uid
        self.am_ticker           = am_contract.symbol
        self.am_exchange         = am_contract.exchange
        self.am_cty_symbol       = am_contract.currency
        self.am_primary_exchange = am_contract.primaryExchange
        self.am_prices_list      = []
        self.am_prices_type_list = []
        self.am_prices_dt_list   = []
  
        return 
    
    #
    # Get methods
    #
    
    # Generic methods that should exist for all agents
    
    def get_uid(self):
        
        #
        # 
        result = self.am_uid
        
        return result
    
    # Specific methods for AgentMonitor

    def get_am_uid(self):
        
        result = self.am_uid
        
        return result
    
    
    def get_am_ticker(self):
        
        result = self.am_ticker
        
        return result

    
    def get_am_exchange(self):
        
        result = self.at_exchange
        
        return result


    def get_am_cty_symbol(self):
        
        result = self.am_cty_symbol
        
        return result
    
    
    def get_am_primary_exchange(self):
        
        result = self.am_primary_exchange
        
        return result

    
    def get_am_prices_list(self):
        
        result = self.am_prices_list
        
        return result


    def get_am_last_price(self):
        
        result = self.am_prices_list[0]
        
        return result


    def get_am_prices_type_list(self):
        
        result = self.am_prices_type_list
        
        return result
    
 
    def get_am_last_price_type(self):
        
        result = self.am_prices_type_list[0]
        
        return result
 
 
    def get_am_prices_dt_list(self):
        
        result = self.am_prices_dt_list
        
        return result
    
    
    def get_am_last_price_dt(self):
        
        result = self.am_prices_dt_list[0]
        
        return result
   
    #
    # Set methods
    #
    
    # Generic methods that should exist for all agents
    
    def set_uid(self, uid):
        
        self.am_uid = uid
        
        return
 

    # Specific methods for AgentMonitor
 
    def set_am_uid(self, am_uid):
        
        self.am_uid = am_uid
        
        return


    def set_am_ticker(self, am_ticker):
        
        self.am_ticker = am_ticker
        
        return

   
    def set_am_exchange(self, am_exchange):
        
        self.am_exchange = am_exchange
        
        return


    def set_am_cty_symbol(self, am_cty_symbol):
        
        self.am_cty_symbol = am_cty_symbol
        
        return
    
    
    def set_am_primary_exchange(self, am_primary_exchange):
        
        self.am_primary_exchange = am_primary_exchange
        
        return
    
 
    def set_am_prices_list(self, am_prices_list):
        
        self.am_prices_list = am_prices_list
        
        return

 
    def set_am_prices_type_list(self, am_prices_type_list):
        
        self.am_prices_type_list = am_prices_type_list
        
        return  


    def set_am_prices_dt_list(self, am_prices_dt_list):
        
        self.am_prices_dt_list = am_prices_dt_list
        
        return  

    
    #
    # Other methods
    #
    
    def notify(self, the_data):
        
        print(" ")
        print("agent_monitor - ", self.am_ticker, ':', the_data)
        
        #
        # store the price and type
        #
        self.am_prices_list.append(the_data.price)
        self.am_prices_type_list.append(the_data.tickType)
        
        # NB we are currently ignoring the price date/time needs fixing
        #am_prices_dt_list
        
        #
        # now tell anyone subscribed to this specific price it has changed
        #
        
        
        return 


    def display(self):
        
        #
        # Display the state of the agent
        #
    
        print('AgentMonitor     : ', self.am_uid)
        print('-----------------')
        print('Ticker           : ', self.am_ticker)
        print('Exchange         : ', self.am_exchange)
        print('Currency         : ', self.am_cty_symbol)
        print('Primary exchange : ', self.am_primary_exchange)
        print('Price list       : ', self.am_prices_list)
        print('Price type list  : ', self.am_prices_type_list)
        print('Price dt list    : ', self.am_prices_dt_list)
            
        return

    
    def __repr__(self):
        
        return str(self.am_uid)
    
    
    def __str__(self): 
        
        return str(self.am_uid)
   

    def __unicode__(self): 
        
        return str(self.am_uid)
    