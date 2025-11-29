#------------------------------------------------------------
# Filename  : AgentTrade.py
# Project   : ava
#
# Descr     : This holds routines relating to the Trade Agent class and methods
#             This is a dumb agent for test purposes that simply waits until the price reaches 
#             the target price then creates a sale instruction
# 
# Params    : None
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-09-03   1 DW  Initial write (rewrite of old version)
# ...
# 2021-12-19 100 DW  Added version 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------


from utils.config import ACT_BUY, ACT_SELL, TYP_MKT, TYP_LMT, PRI_ASK, PRI_BID, TIF_GTC
from infrastructure.blackboard.Price            import Price
from infrastructure.blackboard.TradeInstruction import TradeInstruction


#
# Class definition
#

class AgentTrade:
    
    def __init__(self, at_uid, at_ticker, at_exchange, at_cty_symbol, at_target_profit):
        
        self.at_uid               = at_uid
        self.at_ticker            = at_ticker
        self.at_exchange          = at_exchange
        self.at_cty_symbol        = at_cty_symbol
        self.at_units_held        = 0
        self.at_average_price     = 0
        self.at_target_profit     = at_target_profit
        self.at_last_bid_price    = 0
        self.at_bid_price_list    = [] # latest price first in list
        self.at_bid_price_dt_list = [] # latest date/time for price first in list
        self.at_last_ask_price    = 0
        self.at_ask_price_list    = [] # latest price first in list
        self.at_ask_price_dt_list = [] # latest date/time for price first in list
      
        return 
    
    
    #
    # Get methods
    #
    
    # Generic methods that should exist for all agents
    
    def get_uid(self):
        
        #
        # 
        result = self.at_uid
        
        return result
    
    # Specific methods for AgentTrade

    def get_at_uid(self):
        
        result = self.at_uid
        
        return result
    
    
    def get_at_ticker(self):
        
        result = self.at_ticker
        
        return result

    
    def get_at_exchange(self):
        
        result = self.at_exchange
        
        return result


    def get_at_cty_symbol(self):
        
        result = self.at_cty_symbol
        
        return result

    
    def get_at_units_held(self):
        
        result = self.at_units_held
        
        return result
    
    
    def get_at_average_price(self):
        
        result = self.at_average_price
        
        return result


    def get_at_target_profit(self):
        
        result = self.at_target_profit
        
        return result


    def get_at_last_bid_price(self):
        
        result = self.at_last_bid_price
        
        return result


    def get_at_bid_price_list(self):
        
        result = self.at_bid_price_list
        
        return result


    def get_at_bid_price_dt_list(self):
        
        result = self.at_bid_price_dt_list
        
        return result


    def get_at_last_ask_price(self):
        
        result = self.at_last_ask_price
        
        return result


    def get_at_ask_price_list(self):
        
        result = self.at_ask_price_list
        
        return result


    def get_at_ask_price_dt_list(self):
        
        result = self.at_ask_price_dt_list
        
        return result
    
    
    #
    # Set methods
    #
    
    # Generic methods that should exist for all agents
    
    def set_uid(self, uid):
        
        self.at_uid = uid
        
        return


    # Specific methods for AgentTrade
 
    def set_at_uid(self, at_uid):
        
        self.at_uid = at_uid
        
        return


    def set_at_ticker(self, at_ticker):
        
        self.at_ticker = at_ticker
        
        return

   
    def set_at_exchange(self, at_exchange):
        
        self.at_exchange = at_exchange
        
        return


    def set_at_cty_symbol(self, at_cty_symbol):
        
        self.at_cty_symbol = at_cty_symbol
        
        return
    

    def set_at_units_held(self, at_units_held):
        
        self.at_units_held = at_units_held
        
        return


    def set_at_average_price(self, at_average_price):
        
        self.at_average_price = at_average_price
        
        return


    def set_at_target_profit(self, at_target_profit):
        
        self.at_target_profit = at_target_profit
        
        return


    def set_at_last_bid_price(self, at_last_bid_price):
        
        self.at_last_bid_price = at_last_bid_price
        
        return


    def set_get_at_bid_price_list(self, get_at_bid_price_list):
        
        self.get_at_bid_price_list = get_at_bid_price_list
        
        return


    def set_at_last_ask_price(self, at_last_ask_price):
        
        self.at_last_ask_price = at_last_ask_price
        
        return


    def set_at_ask_price_list(self, at_ask_price_list):
        
        self.at_ask_price_list = at_ask_price_list
        
        return
    
    
    def set_at_ask_price_dt_list(self, at_ask_price_dt_list):
        
        self.at_ask_price_dt_list = at_ask_price_dt_list
        
        return
  

    #
    # Other methods
    #
    
    def notify(self, the_data):
        
        #
        # process the notification object check its type
        #
          
        if isinstance(the_data, Price):
  
            new_price = the_data
            
            if (new_price.get_pri_ticker()   == self.at_ticker   and 
                new_price.get_pri_exchange() == self.at_exchange):
      
                #
                # the price is for the ticker and exchange we care about so record 
                # the latest price in the relevant places
                #
               
                price          = new_price.get_pri_price()
                price_dt       = new_price.get_pri_dt() 
                new_price_type = new_price.get_pri_type()  
          
                if new_price_type == PRI_BID:
                    
                    self.at_last_bid_price = price
                    self.at_bid_price_list.insert(price)       # add to front of list
                    self.at_bid_price_dt_list.insert(price_dt) # add to front of list
                    
                    #
                    # Now see if we want to make a trade decision based on the new bid price
                    #
                    
                    self.process_new_price(new_price)
                
                elif new_price_type == PRI_ASK:
                    
                    self.at_last_ask_price = price
                    self.at_ask_price_list.insert(price)       # add to front of list
                    self.at_ask_price_dt_list.insert(price_dt) # add to front of list
            
        return 
    

    def display(self):
        
        #
        # Display the state of the agent
        #
    
        print('AgentTrade        : ', self.at_uid)
        print('------------------')
        print('Ticker            : ', self.at_ticker)
        print('Exchange          : ', self.at_exchange)
        print('Currency          : ', self.at_cty_symbol)
        print('Units held        : ', self.at_units_held)
        print('Average price     : ', self.at_average_price)
        print('Target profit     : ', self.at_target_profit)
        print('Last bid price    : ', self.at_last_bid_price)
        print('Bid price list    : ', self.at_bid_price_list)
        print('Bid price dt list : ', self.at_bid_price_dt_list)
        print('Last ask price    : ', self.at_last_ask_price)
        print('Ask price list    : ', self.at_ask_price_list)
        print('Ask price dt list : ', self.at_ask_price_dt_list)
            
        return


    def process_new_price(self, new_price):
        
        #
        # Check if the price is for the equity we are trading
        #
             
        if (new_price.get_pri_ticker()   == self.at_ticker   and 
            new_price.get_pri_exchange() == self.at_exchange):
            
            #
            # It is so record the price and see if we want to do something with it
            #
            
            self.record_price(new_price)
            
            if (self.at_units_held == 0):
 
                #
                # we have no holding so buy some 
                #
           
                ti_order_id     = "" 
                ti_ticker       = self.at_ticker
                ti_exchange     = self.at_exchange
                ti_order_action = ACT_BUY
                ti_order_type   = TYP_MKT
                ti_order_tif    = TIF_GTC
                ti_order_qty    = 100
                ti_descr        = '{} {} {}.{} {} {}'.format(ti_order_action, ti_order_qty, ti_ticker, ti_exchange, ti_order_tif)
       
                trade_instruction = TradeInstruction(ti_order_id     = ti_order_id, 
                                                     ti_descr        = ti_descr, 
                                                     ti_ticker       = ti_ticker, 
                                                     ti_exchange     = ti_exchange, 
                                                     ti_order_action = ti_order_action,
                                                     ti_order_type   = ti_order_type,
                                                     ti_order_tif    = ti_order_tif, 
                                                     ti_order_qty    = ti_order_qty)
        
                return trade_instruction
            
            else:
                
                #
                # We have some so see if we have met target price
                #
                
                new_price_type = new_price.get_pri_type()  
                
                if new_price_type == PRI_BID:
                    
                    bid_price     = new_price.get_pri_price()
                    average_price = self.get_at_average_price() 
                    current_units = self.get_at_units_held()
                    
                    target_profit           = self.get_at_target_profit()
                    target_profit_per_share = target_profit / current_units 
                    
                    if (bid_price >= average_price + target_profit_per_share):
                        
                        #
                        # We should sell the holding
                        #
                   
                        ti_order_id     = "" 
                        ti_ticker       = self.at_ticker
                        ti_exchange     = self.at_exchange
                        ti_order_action = ACT_SELL
                        ti_order_type   = TYP_LMT
                        ti_order_tif    = TIF_GTC
                        ti_order_qty    = self.get_at_units_held
                        ti_descr        = '{} {} {}.{} {} {}'.format(ti_order_action, ti_order_qty, ti_ticker, ti_exchange, ti_order_tif)
       
                        trade_instruction = TradeInstruction(ti_order_id     = ti_order_id, 
                                                             ti_descr        = ti_descr, 
                                                             ti_ticker       = ti_ticker, 
                                                             ti_exchange     = ti_exchange, 
                                                             ti_order_action = ti_order_action, 
                                                             ti_order_type   = ti_order_type,
                                                             ti_order_tif    = ti_order_tif, 
                                                             ti_order_qty    = ti_order_qty)
        
                        return trade_instruction
                    
        return

    
    def __repr__(self):
        
        return str(self.at_uid)
    
    
    def __str__(self): 
        
        return str(self.at_uid)
   

    def __unicode__(self): 
        
        return str(self.at_uid)
    