#------------------------------------------------------------
# Filename  : agent_action_apply_rule.py
# Project   : ava
#
# Descr     : This holds routines to load apply a rule
#
# Params    : database
#             rule_name
#             ticker
#             start_datetime
#             end_datetime
#             num_items
#             freq_type
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-05-05   1 MW  Initial write
#
# NOTE That this does not actually work yet - checking in so it's not lost
# ...
# 2021-12-19 100 DW  Added version 
# 2022-10-29 200 DW  Reorg
#------------------------------------------------------------

from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import close_db, open_db
from agents.agent_rule_simple_change import Agent_rule_simple_change, add_new_item, get_next_action 
from database.db_objects.ImsHistMktDataDB import get_last_bid_ask_price_in_range


def agent_action_apply_rule(database, rule_name, ticker, start_datetime, end_datetime, num_items, freq_type):
    
    print('===========================================================================================')   
    print(' Start of action_apply_rule processing')
    print(' Testing rule:', rule_name,'for Ticker',ticker,'between', start_datetime,'and', end_datetime)
    print(' Num_items', num_items, 'freq_type', freq_type)
    print('===========================================================================================')   


    # first set up the dictionary to take the transactions you'll be building
    transaction_list = []
    last_action = 'START'

    # Then instantiate the rule class
    if rule_name == 'SIMPLE_CHANGE':
        
        new_rule = Agent_rule_simple_change(num_items = num_items)
        
        
        # Get data stream of prices to suit the specific rule
        
        data_stream = get_last_bid_ask_price_in_range(database           = database, 
                                                     hmd_inv_ticker     = ticker, 
                                                     hmd_start_datetime = start_datetime, 
                                                     hmd_end_datetime   = end_datetime, 
                                                     hmd_freq_type      = '1 min')
        
            
        for hmd_start_datetime, hmd_last_bid_price, hmd_last_ask_price in data_stream:
            
            #print('>>>>> adding', hmd_last_bid_price)
            add_new_item(new_rule, hmd_last_bid_price)
            
            new_action = get_next_action(new_rule)
            
            if new_action != 'NOTHING':
                if last_action == 'START':
                    # next action must be 'BUY':
                    
                    if new_action == 'BUY':
                        # can now record the transaction
                        transaction_list = record_this_transaction(transaction_list = transaction_list, 
                                                                   this_action      = new_action, 
                                                                   this_datetime    = hmd_start_datetime, 
                                                                   this_price       = hmd_last_bid_price)
                        last_action = new_action
                
                        
                else:
                    # next action must be different to the last
                    if last_action != new_action:
                        # can now record the transaction
                        
                        if new_action == 'BUY':
                            transaction_list = record_this_transaction(transaction_list = transaction_list, 
                                                                       this_action      = new_action, 
                                                                       this_datetime    = hmd_start_datetime, 
                                                                       this_price       = hmd_last_bid_price)
                        else:
                            transaction_list = record_this_transaction(transaction_list = transaction_list, 
                                                                       this_action      = new_action, 
                                                                       this_datetime    = hmd_start_datetime, 
                                                                       this_price       = hmd_last_bid_price)
                        last_action = new_action
                
                          
    
    #print(transaction_list)            
    total_profit = calculate_profit(transaction_list)
    
    print(' ')
    print('=======================================================================')   
    print(' End of action_apply_rule processing - total_profit', total_profit)
    print('=======================================================================')   
    
    return


def record_this_transaction(transaction_list, this_action, this_datetime, this_price):
    '''
    this_transaction = {
        "type":     this_action,
        "datetime": this_datetime,
        "price":    this_price
        }
    '''
    this_transaction = [this_action, this_datetime, this_price]
     
    print('####### this_transaction', this_transaction)
    
    transaction_list.append(this_transaction)
    #print(transaction_list)
    
    return transaction_list


def calculate_profit(transaction_list):
    
    total_profit = 0
    last_price = 0
    
    for this_rec in transaction_list:
        
        this_type     = this_rec[0]
        this_datetime = this_rec[1]
        this_price    = this_rec[2]
        if this_type == 'BUY':
            last_price = this_price
        else:
            this_profit = this_price - last_price
            total_profit += this_profit

    
    return total_profit


if __name__ == "__main__":

    print("Open db")
    print(" ")
    
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)
    
    ticker         = 'CM.TO'
    rule_name      = 'SIMPLE_CHANGE'
    start_datetime = '2016-01-01 06:30:00'
    end_datetime   = '2019-12-31 12:59:59'
    num_items      = 60  # represents 1 hour if freq_type = '1 min'
    freq_type      = '1 min'
    
    agent_action_apply_rule(database       = database,
                            rule_name      = rule_name,
                            ticker         = ticker,
                            start_datetime = start_datetime,
                            end_datetime   = end_datetime,
                            num_items      = num_items,
                            freq_type      = freq_type)
       
    print(" ")
    print("Close db")
    print(" ")
    
    close_db(database = database)  
    

