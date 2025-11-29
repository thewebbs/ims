#------------------------------------------------------------
# Filename  : AgentHistPrices.py
# Project   : ava
#
# Descr     : This holds routines relating to the Historic Market Prices Agent class
#
# Params    : None
#         
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-09-17   1 DW  Initial write
# ...
# 2021-12-19 100 DW  Added version 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from utils.config import IB_API_PORT, IB_API_GATEWAY_IP

from ibapi.client   import EClient
from ibapi.wrapper  import EWrapper
from ibapi.contract import Contract
from ibapi.ticktype import TickTypeEnum


    
#
# Class definition
#

class AgentHistPrices:
    
    def __init__(self, hp_uid, hp_contract, hp_end_date, hp_duration, hp_frequency, hp_price_type):
        
        self.hp_uid              = hp_uid
        self.hp_ticker           = hp_contract.symbol
        self.hp_exchange         = hp_contract.exchange
        self.hp_cty_symbol       = hp_contract.currency
        self.hp_primary_exchange = hp_contract.primaryExchange
        self.hp_end_date         = hp_end_date
        self.hp_duration         = hp_duration
        self.hp_frequency        = hp_frequency
        self.hp_price_type       = hp_price_type
        
        return 
    
    #
    # Get methods
    #
    
    # Generic methods that should exist for all agents
    
    def get_uid(self):
        
        #
        # 
        result = self.hp_uid
        
        return result
    
    
    # Specific methods for AgentHistPrices

    def get_hp_uid(self):
        
        result = self.hp_uid
        
        return result
    
    
    def get_hp_ticker(self):
        
        result = self.hpticker
        
        return result

    
    def get_hp_exchange(self):
        
        result = self.hp_exchange
        
        return result


    def get_hp_cty_symbol(self):
        
        result = self.hp_cty_symbol
        
        return result


    def get_hp_frequency(self):
        
        result = self.hp_frequency
        
        return result
 
   
    #
    # Set methods
    #
    
    # Generic methods that should exist for all agents
    
    def set_uid(self, uid):
        
        self.hp_uid = uid
        
        return
 

    # Specific methods for AgentHistPrices
 
    def set_hp_uid(self, hp_uid):
        
        self.hp_uid = hp_uid
        
        return


    def set_hp_ticker(self, hp_ticker):
        
        self.hp_ticker = hp_ticker
        
        return

   
    def set_hp_exchange(self, hp_exchange):
        
        self.hp_exchange = hp_exchange
        
        return


    def set_hp_cty_symbol(self, hp_cty_symbol):
        
        self.hp_cty_symbol = hp_cty_symbol
        
        return
    

    def set_hp_frequency(self, hp_frequency):
        
        self.hp_frequency = hp_frequency
        
        return

    
    #
    # Other methods
    #
    
    def notify(self, the_data):
        
        print("agent_hist_prices - ", self.hp_ticker, ':', the_data)
        print(" ")
       
        #
        # store the price and type
        #
        #self.am_prices_list.append(the_data.price)
        #self.am_prices_type_list.append(the_data.tickType)
        
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
    
        print('AgentHistPrices : ', self.hp_uid)
        print('----------------')
        print('Ticker          : ', self.hp_ticker)
        print('Exchange        : ', self.hp_exchange)
        print('Currency        : ', self.hp_cty_symbol)
        print('Frequency       : ', self.hp_frequency)
            
        return

    
    def __repr__(self):
        
        return str(self.hp_uid)
    
    
    def __str__(self): 
        
        return str(self.hp_uid)
   

    def __unicode__(self): 
        
        return str(self.hp_uid)
    
    
    
    