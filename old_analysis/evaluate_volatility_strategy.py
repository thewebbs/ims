#------------------------------------------------------------
# Filename  : evaluate_volatility_strategy.py
# Project   : ava
#
# Descr     : This file contains code to evaluate the volatility StrategyDB
#
# Params    : None
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2017-06-06   1 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from database.db_objects.CandlestickDB import get_last_midpoint_in_range
from utils.config import DEBUG, VISUAL_COLOR_LIST, VISUAL_GRAPH_KIND, VISUAL_LEGEND_BBOX_TO_ANCHOR, VISUAL_LEGEND_BBOX_WIDTH, VISUAL_LEGEND_COLS
from utils.config import VISUAL_LEGEND_FANCYBOX, VISUAL_LEGEND_FONT_SIZE, VISUAL_GREY_LINE_COLOR, VISUAL_LINE_STYLE, VISUAL_LINE_WEIGHT, VISUAL_MARKER_EDGE_COLOR
from utils.config import VISUAL_MARKER_KIND, VISUAL_STYLE_TO_USE, VISUAL_TOP_TITLE_FONT_SIZE, VISUAL_XAXIS_COLOR, VISUAL_YAXIS_COLOR, VISUAL_XAXIS_ROTATION
from utils.database_mysql import close_db, open_db 
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from utils.utils_dataframes import format_midpoint_df
#from matplotlib.cbook import Null
#from matplotlib.patches import ArrowStyle
from datetime import datetime



# Get config_data depending on tickers - these can be overriden by the calling routine

def get_default_config_data(algorithm_name, inv_ticker, inv_exc_symbol):
    
    if (algorithm_name == 'VOLATILITY1') or (algorithm_name == 'VOLATILITY2') or (algorithm_name == 'VOLATILITY3'):
         
        config_dict = {}
        
        # Note these are the default parameters to be used for this algorithm for this inv_ticker and inv_exchange_symbol
        
        if (inv_ticker == 'CM') and (inv_exc_symbol == 'TSE'):
            inv_ticker_exc_symbol = 'CM.TO'
            units_to_go_long = 100 
            units_to_go_short = 100
            threshold_price_increase = 0.30
            threshold_price_drop = 0.30
          
        if (inv_ticker == 'BMO') and (inv_exc_symbol == 'TSE'):
            inv_ticker_exc_symbol = 'BMO.TO'
            units_to_go_long = 100
            units_to_go_short = 100
            threshold_price_increase = 0.20
            threshold_price_drop = 0.20
        
        if (inv_ticker == 'TD') and (inv_exc_symbol == 'TSE'):
            inv_ticker_exc_symbol = 'TD.TO'
            units_to_go_long = 100
            units_to_go_short = 100
            threshold_price_increase = 0.15
            threshold_price_drop = 0.15
        
        if (inv_ticker == 'RY') and (inv_exc_symbol == 'TSE'):
            inv_ticker_exc_symbol = 'RY.TO'
            units_to_go_long = 100
            units_to_go_short = 100
            threshold_price_increase = 0.50
            threshold_price_drop = 0.50
        
        if (inv_ticker == 'BNS') and (inv_exc_symbol == 'TSE'):
            inv_ticker_exc_symbol = 'BNS.TO'
            units_to_go_long = 100
            units_to_go_short = 100
            threshold_price_increase = 0.50
            threshold_price_drop = 0.50
        
        if (inv_ticker == 'QSP.UN') and (inv_exc_symbol == 'TSE'):
            inv_exc_symbol = 'TSE'
            inv_ticker_exc_symbol = 'QSP.UN.TO'
            units_to_go_long = 100
            units_to_go_short = 100
            threshold_price_increase = 0.15
            threshold_price_drop = 0.15
                
        new_dict = {inv_ticker_exc_symbol :  [inv_ticker,
                                              inv_exc_symbol, 
                                              units_to_go_long, 
                                              units_to_go_short,
                                              threshold_price_increase,
                                              threshold_price_drop]}
        config_dict.update(new_dict)     
              
    
    return config_dict



# ALGORITHM section

def evaluate_strategy(algorithm_name, start_date, end_date, freq_type, output_name, override_config_dict, visualize):

    if (algorithm_name == 'VOLATILITY1') or (algorithm_name == 'VOLATILITY2') or (algorithm_name == 'VOLATILITY3'):
    
    
        # first get the default config for this algorithm for each of the tickers and exchange symbols supplied
        # unpack each of the config items, one at a time for processing
        
        combined_config_dict = {}
        new_dict = {}
        
        for key in override_config_dict:
            
            values = override_config_dict[key]
            inv_ticker = values[0]
            inv_exc_symbol = values[1]
            override_units_to_go_long = values[2]
            override_units_to_go_short = values[3]
            override_threshold_price_increase = values[4]
            override_threshold_price_drop = values[5]
    
            default_config_dict = get_default_config_data(algorithm_name, inv_ticker, inv_exc_symbol)
            
            
            # pick up default values but may override
            
            default_values = default_config_dict[key]
            units_to_go_long = default_values[2]
            units_to_go_short = default_values[3]
            threshold_price_increase = default_values[4]
            threshold_price_drop = default_values[5]
            
            
            if override_units_to_go_long is not None:
                units_to_go_long = override_units_to_go_long
                
            if override_units_to_go_short is not None:
                units_to_go_short = override_units_to_go_short
                
            if override_threshold_price_increase is not None:
                threshold_price_increase = override_threshold_price_increase
                
            if override_threshold_price_drop is not None:
                threshold_price_drop = override_threshold_price_drop
            
            new_dict = {key :  [inv_ticker,
                                                  inv_exc_symbol, 
                                                  units_to_go_long, 
                                                  units_to_go_short,
                                                  threshold_price_increase,
                                                  threshold_price_drop]}
    
            combined_config_dict.update(new_dict)   
        
        
    # now we have the correct config information evaluate the result    
    
    results_dict = examine_price_changes(algorithm_name,
                                         start_date,
                                         end_date,
                                         freq_type,
                                         combined_config_dict,
                                         output_name,
                                         visualize)




    

    
    return results_dict 


#------------------------------------------------------------------------------------------------------

# Supporting routines


# examine the price changes to identify times to hold short/long positions, frequency and profit/loss

def examine_price_changes(algorithm_name, start_date, end_date, freq_type, config_dict, output_name, visualize):
    
    # common stuff first
    
    # initialize dictionary to hold results
    results_dict = {}
    
    
    if  (algorithm_name == 'VOLATILITY1') or (algorithm_name== 'VOLATILITY2') or (algorithm_name== 'VOLATILITY3'):
        
            
        # unpack each of the config items, one at a time for processing
        for key in config_dict:
            values = config_dict[key]
            inv_ticker = values[0]
            inv_exc_symbol = values[1]
            units_to_go_long = values[2]
            units_to_go_short = values[3]
            threshold_price_increase = values[4]
            threshold_price_drop = values[5]
            
        
            print("%s Examining price changes for %s %s %s %s %s %s from %s to %s" % 
                  (
                  algorithm_name,
                  inv_ticker, 
                  inv_exc_symbol,
                  units_to_go_long, 
                  units_to_go_short, 
                  threshold_price_increase, 
                  threshold_price_drop,
                  start_date,
                  end_date
                  ))
    
    
            # initialize dictionaries to hold positions
            current_long_positions = {}    
            current_short_positions = {}
            
            # initialize dictionary to hold transactions and dataframes for buy/sell transactions
            transactions_list = {}
            full_trans_list = {}
            
            #initialize variables recording holdings
            number_opportunities = 0
            total_profit_loss = 0   
            reached_long_threshold = False
            price_at_threshold = 0
            
        
            # get last bid offer prices for this investment in the date period
            midpoint_data = get_last_midpoint_in_range(inv_ticker, inv_exc_symbol, start_date, end_date, freq_type)    
            
            
            # Amended to calculate mid point between bid and offer rather than two separate lines for the graphs - only if graphing
            
            if len(midpoint_data) > 0:
                    
                for start_datetime, bid_price, ask_price, mid_price in midpoint_data:
                        
                    new_long_positions, new_short_positions, transactions_list = should_take_new_positions(algorithm_name, \
                                                         current_long_positions, current_short_positions, \
                                                         transactions_list, inv_ticker, inv_exc_symbol, 
                                                         units_to_go_long, units_to_go_short, \
                                                         start_datetime, bid_price, ask_price, \
                                                         threshold_price_increase, threshold_price_drop)
                        
                    current_long_positions = new_long_positions
                    current_short_positions = new_short_positions
                        
                    new_long_positions, new_short_positions, transactions_list, reached_long_threshold, price_at_threshold, full_trans_list  = should_change_positions(algorithm_name,\
                                                         current_long_positions, current_short_positions, \
                                                         transactions_list, inv_ticker, inv_exc_symbol, \
                                                         units_to_go_long, units_to_go_short, \
                                                         start_datetime, bid_price, ask_price, \
                                                         threshold_price_increase, threshold_price_drop, \
                                                         reached_long_threshold, price_at_threshold,
                                                         full_trans_list)
                    current_long_positions = new_long_positions
                    current_short_positions = new_short_positions
                      
        
        
                number_opportunities, total_profit_loss = evaluate_results(transactions_list)
                
                # save results in the dictionary for returning
                new_dict = {key : [inv_ticker, inv_exc_symbol, number_opportunities, total_profit_loss]}
                results_dict.update(new_dict) 
                
                if DEBUG:
                    print("evaluation completed for ", inv_ticker, inv_exc_symbol)
                print("-------------------------------")
                
                
                
                # graph midpoint prices if we re graphing:
                
                if visualize:
                    
                    mid_df = format_midpoint_df(inv_ticker, inv_exc_symbol, start_date, end_date, freq_type)
                
                    print_datetime = datetime.today() 
                    print(print_datetime, 'About to graph results')
                    graph_results(algorithm_name, start_date, end_date, config_dict, mid_df, full_trans_list)
                    print_datetime = datetime.today() 
                    print(print_datetime, 'After graphing results')
                        
        
            else:
                print("ERROR: no matching price data found for ", inv_ticker, inv_exc_symbol)
                print("-------------------------------")
    
         
    return results_dict


# Evaluate the results of the StrategyDB and report back




def evaluate_results(transactions_list):
    
    if DEBUG:
        print("in evaluate results")
        
    total_profit_loss = 0
    
    if (algorithm_name == 'VOLATILITY1') or (algorithm_name == 'VOLATILITY2') or (algorithm_name == 'VOLATILITY3'):
    
        for key in range(len(transactions_list)):
            key += 1
            values = transactions_list[key]
            buy_or_sell = values[0] 
            #inv_ticker = values[1]
            #inv_exc_symbol = values[2]
            units = values[3]
            price = values[4]
            original_price = values[5]
            difference = values[6]
            original_date = values[7]
            this_date = values[8]
            
            if difference == 0:
                print('Type %s - New TransactionDB - date %s and price %5.2f ' % (buy_or_sell, this_date, price))
            else:
                print('Type %s Original date %s and price %5.2f - this date %s and price %5.2f - difference = %5.2f' % (buy_or_sell, original_date, original_price, this_date, price, difference*units))
             
            total_profit_loss += (difference * units)
            
        # number of opportunities excludes the initial two transactions - still think this number is wrong
        number_opportunities = len(transactions_list) - 2
            
    return number_opportunities, total_profit_loss


def should_change_positions(algorithm_name, current_long_positions, current_short_positions, transactions_list, inv_ticker, inv_exc_symbol, units_to_go_long, units_to_go_short, start_datetime, bid_price, ask_price,  threshold_price_increase, threshold_price_drop, reached_long_threshold, price_at_threshold, full_trans_list):
      
    
    number_transactions = len(transactions_list)
    number_full_trans = len(full_trans_list)
    #format_date = start_datetime.strftime('%y%m%d %H:%M:%S')
                  
    
    if (algorithm_name == 'VOLATILITY1') or (algorithm_name == 'VOLATILITY2'):
            
            
        # if we hold a short PositionDB - compare price of original short PositionDB to latest bid price
        
        if len(current_short_positions) > 0:
            
            # NOTE we could have more than one short PositionDB so using FIFO we look at the oldest
            earliest_key = min(current_short_positions, key=current_short_positions.get)
            earliest_rec = current_short_positions[earliest_key]
            original_units = earliest_rec[2]
            original_price = earliest_rec[3]
            original_datetime = earliest_rec[6]
            
            difference = original_price - ask_price
            
                
            
            if DEBUG:
                print("original %5.2f ask_price %5.2f difference %5.2f threshold drop %5.2f " % (original_price, ask_price, difference, threshold_price_drop))
            
            if difference > threshold_price_drop:
                
                # NOTE: TransactionDB list has original price and difference for later use
                new_dict = {number_transactions+1: ['BUY', inv_ticker, inv_exc_symbol, units_to_go_long, ask_price, original_price, difference, earliest_key, start_datetime, 'SHORT', original_datetime ]}
                transactions_list.update(new_dict) 
                
                new_full_dict = {number_full_trans+1: ['SHORT', 'BUY', inv_ticker, inv_exc_symbol, original_units, original_price, original_datetime, units_to_go_long, ask_price, start_datetime ]}
                full_trans_list.update(new_full_dict) 
                
                number_full_trans += 1
                number_transactions += 1
                
                # if there is only one record in the short positions dictionary then null it, otherwise remove this record
                num_in_short_dict = len(current_short_positions.keys())
                if num_in_short_dict == 1:
                    current_short_positions = {} 
                else:
                    del current_short_positions[earliest_key]
                    
                
                   
        # if we hold a long PositionDB - compare price of original long PositionDB to latest ask price
        
        if len(current_long_positions) > 0:
            
            # NOTE we could have more than one long PositionDB so using FIFO we look at the oldest
            earliest_key = min(current_long_positions, key=current_long_positions.get)
            earliest_rec = current_long_positions[earliest_key]
            original_units = earliest_rec[2]
            original_price = earliest_rec[3]
            original_datetime = earliest_rec[6]
            
            difference = bid_price - original_price
            
            if DEBUG:
                print("original %5.2f bid_price %5.2f difference %5.2f threshold increase %5.2f" % (original_price, bid_price, difference, threshold_price_increase))
            
            if difference > threshold_price_increase:
                
                # NOTE: TransactionDB list has original price and difference for later use
                new_dict = {number_transactions+1: ['SELL', inv_ticker, inv_exc_symbol, units_to_go_long, bid_price, original_price, difference, earliest_key, start_datetime, 'LONG', original_datetime ]}
                transactions_list.update(new_dict) 
                
                new_full_dict = {number_full_trans+1: ['LONG', 'SELL', inv_ticker, inv_exc_symbol, original_units, original_price, original_datetime, units_to_go_long, bid_price, start_datetime ]}
                full_trans_list.update(new_full_dict) 
                
                number_full_trans += 1
                number_transactions += 1
                
                # if there is only one record in the long positions dictionary then null it, otherwise remove this record
                num_in_long_dict = len(current_short_positions.keys())
                if num_in_long_dict == 1:
                    current_long_positions = {} 
                else:
                    del current_long_positions[earliest_key]
                    
                
    
    #######################################

                    
    if algorithm_name == 'VOLATILITY3':
            
        # if we hold a long PositionDB - compare price of original long PositionDB to latest ask price
        
        if len(current_long_positions) > 0:
            
            # NOTE we could have more than one long PositionDB so using FIFO we look at the oldest
            earliest_key = min(current_long_positions, key=current_long_positions.get)
            earliest_rec = current_long_positions[earliest_key]
            original_units = earliest_rec[2]
            original_price = earliest_rec[3]
            original_datetime = earliest_rec[6]
            
            difference = ask_price - original_price
            twice_threshold = 2 * threshold_price_increase
            twice_threshold_test = float(difference) - twice_threshold
            
            if DEBUG:
                print('##')
                print("original %5.2f ask_price %5.2f bid price %5.2f difference %5.2f threshold increase %5.2f" % (original_price, ask_price, bid_price, difference, threshold_price_increase))
            
            if ((reached_long_threshold == False) and (difference > threshold_price_increase)) or (reached_long_threshold == True):
                
                    if (reached_long_threshold == False) and (difference > threshold_price_increase):
                        reached_long_threshold = True
                        price_at_threshold = bid_price
                        if DEBUG:
                            print('we have just reached threshold but do not want to sell immediately so start monitoring instead - bid price at threshold =', price_at_threshold)
                    
                    difference_to_threshold = price_at_threshold - bid_price  
                    
                    if DEBUG:
                        print('          price_at_threshold',price_at_threshold,'difference_to_threshold',difference_to_threshold)
                    
                    if (difference_to_threshold > 0.1) or (twice_threshold_test >= 0): 
                        if DEBUG:
                            if (difference_to_threshold > 0.1) :
                                print('          price has now fallen so selling - difference to threshold = ',difference_to_threshold)
                            
                            if (twice_threshold_test >= 0):
                                print('          *** reached twice the threshold')
                        
                        # NOTE: TransactionDB list has original price and difference for later use
                        
                        new_dict = {number_transactions+1: ['SELL', inv_ticker, inv_exc_symbol, units_to_go_long, ask_price, original_price, difference, earliest_key, start_datetime, 'LONG', original_datetime ]}
                        transactions_list.update(new_dict) 
                
                        new_full_dict = {number_full_trans+1: ['LONG', 'SELL', inv_ticker, inv_exc_symbol, original_units, original_price, original_datetime, units_to_go_long, ask_price, start_datetime ]}
                        full_trans_list.update(new_full_dict) 
                        
                        
                        # if there is only one record in the long positions dictionary then null it, otherwise remove this record
                        num_in_long_dict = len(current_short_positions.keys())
                        if num_in_long_dict == 1:
                            current_long_positions = {} 
                        else:
                            del current_long_positions[earliest_key]
                            
                        if DEBUG:
                            print('reset reached_long_threshold to False')
                        reached_long_threshold = False
                        price_at_threshold = 0
                        
                    else:
                        if DEBUG:
                            print('          continuing to hold as price is still rising - difference to threshold = ',difference_to_threshold)
                    
               
    
    if DEBUG:
        print("transactions_list = ", transactions_list)            
        print("number_transactions ", number_transactions)


    return current_long_positions, current_short_positions, transactions_list, reached_long_threshold, price_at_threshold, full_trans_list



def should_take_new_positions(algorithm_name, current_long_positions, current_short_positions, transactions_list, inv_ticker, inv_exc_symbol, units_to_go_long, units_to_go_short, start_datetime, bid_price, ask_price, threshold_price_increase, threshold_price_drop):
   
    transaction_type = 'MKT'
    number_transactions = len(transactions_list)
    format_date = start_datetime.strftime('%y%m%d %H:%M:%S')
        
    if algorithm_name == 'VOLATILITY1':
            
        if (len(current_long_positions) == 0) and (len(current_short_positions) == 0):
            
            new_dict = {start_datetime : [inv_ticker, inv_exc_symbol, units_to_go_long, ask_price, transaction_type, 'LONG', start_datetime]}
            current_long_positions.update(new_dict) 
            
            new_dict = {number_transactions+1: ['BUY', inv_ticker, inv_exc_symbol, units_to_go_long, ask_price, 0, 0, 0, start_datetime, 'LONG', start_datetime]}
            transactions_list.update(new_dict) 
            number_transactions += 1
            
            new_dict = {start_datetime : [inv_ticker, inv_exc_symbol, units_to_go_long, bid_price, transaction_type, 'SHORT', start_datetime]}
            current_short_positions.update(new_dict) 
            
            new_dict = {number_transactions+1 : ['SELL', inv_ticker, inv_exc_symbol, units_to_go_long, bid_price, 0, 0, 0, start_datetime, 'LONG', start_datetime]}
            transactions_list.update(new_dict) 
            number_transactions += 1    
            
            
 
    ######################################
    
    if (algorithm_name == 'VOLATILITY2') or (algorithm_name == 'VOLATILITY3'):
                
        # In this algorithm, we only have long positions and no short ones - so if we have a long PositionDB
        # already we clearly don't need another.
        # If we don't have a long PositionDB then we're looking for a drop in price since the last TransactionDB
        
        if (len(current_long_positions) == 0):
            
        
            if number_transactions > 0:
                
                latest_tran_key = min(transactions_list, key=transactions_list.get)
                latest_tran_rec = transactions_list[latest_tran_key]
                latest_price = latest_tran_rec[4]
                difference = latest_price - ask_price  
                
                
            if (number_transactions == 0) or ((number_transactions) > 0 and (difference > threshold_price_drop)):
                
                if (number_transactions) > 0 and (difference > threshold_price_drop):
                
                    if DEBUG:
                        print('What we sold for before', latest_price,'What the price is now',ask_price,'difference', difference,'threshold_price_drop',threshold_price_drop,'date', format_date)
                    
                if DEBUG:
                    print('taking out new long PositionDB')
                
                new_dict = {start_datetime : [inv_ticker, inv_exc_symbol, units_to_go_long, ask_price, transaction_type, 'LONG', start_datetime]}
                current_long_positions.update(new_dict) 
                
                new_dict = {number_transactions+1: ['BUY', inv_ticker, inv_exc_symbol, units_to_go_long, ask_price, 0, 0, 0, start_datetime, 'LONG', start_datetime]}
                transactions_list.update(new_dict) 
                number_transactions += 1
     
             
    return current_long_positions, current_short_positions, transactions_list



def graph_results(algorithm_name, start_date, end_date, config_dict, mid_df, full_trans_list):
    
    #len_config_dict = len(config_dict.keys())
    plt.style.use(VISUAL_STYLE_TO_USE) 
    
    fig, (ax1) = plt.subplots() 
        
    plt.xlabel('Date & Time', color=VISUAL_XAXIS_COLOR) # this sets the axes title colour only
    plt.ylabel('price', color=VISUAL_XAXIS_COLOR) # this sets the axes title colour only
   
    ttitle = "%s Buy/Sell signals between %s and %s" % (algorithm_name, start_date, end_date)
    
    fig.suptitle(ttitle, fontsize=VISUAL_TOP_TITLE_FONT_SIZE)

    
            
    
    #################
    
    # re-form full_trans_list dictionary to create a series of dataframes and ass them to the same plot
    first_time = True
    
    if len(full_trans_list) >= 1:
        
        for key in range(len(full_trans_list)):
         
            key += 1
            values = full_trans_list[key]
            long_or_short = values[0] 
            #buy_or_sell = values[1] 
            inv_ticker = values[2]
            inv_exc_symbol = values[3]
            #original_units = values[4]
            original_price = values[5]
            original_datetime = values[6]
            #new_units = values[7]
            new_price = values[8]
            new_datetime = values[9]
    
            if first_time:
                        
                plot_title = "Analysis for %s.%s" % (inv_ticker, inv_exc_symbol)
                #plot_title = "Analysis " 
                first_time = False
                    
            mid_df.plot(ax=ax1, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color = VISUAL_GREY_LINE_COLOR)
            ax1.set_title(plot_title)
 
            full_trans_df = pd.DataFrame()
        
            # Note one record here is being split into two dataframe records - one for the start of the TransactionDB
            # and the other for the end.
            
            full_tran_index_val = inv_ticker + "-" + inv_exc_symbol + '_' + long_or_short  + '_' + str(key)
            format_date = original_datetime.strftime('%y%m%d %H:%M:%S')
      
            temp_df = pd.DataFrame({format_date: (float(original_price))}, index=[full_tran_index_val] )
            full_trans_df = pd.concat([full_trans_df, temp_df])
                            
            full_tran_index_val = inv_ticker + "-" + inv_exc_symbol + '_' + long_or_short  + '_' + str(key)
            format_date = new_datetime.strftime('%y%m%d %H:%M:%S')
      
            temp_df = pd.DataFrame({format_date: (float(new_price))}, index=[full_tran_index_val] )
            full_trans_df = pd.concat([full_trans_df, temp_df])
    
            full_trans_df = full_trans_df.groupby(full_trans_df.index).sum()
            full_trans_df = pd.DataFrame.transpose(full_trans_df)
            
            
            # Next line creates a new dataframe with the same index as the mid price dataframe so that
            # this can be plotted on same plot as the mid price dataframe and share an index
            new_trans_df = pd.DataFrame(index=mid_df.index)
            
            # next line then puts the data frame this TransactionDB - open and close - onto the same dataframe
            this_trans_df1 = pd.concat([new_trans_df, full_trans_df], axis = 1)

            # next bit plots this new dataframe as markers
            this_trans_df1.plot(ax=ax1, kind=VISUAL_GRAPH_KIND, marker=VISUAL_MARKER_KIND, markeredgecolor=VISUAL_MARKER_EDGE_COLOR, color=VISUAL_COLOR_LIST[key])
            
          
            # next line interpolate - forward which fills in the gap after the first record
            # pro-rating values after it til it reaches the end 
            # sadly it continues to use this value right until the end which the second interpolate statement fixs
            this_trans_df2 = this_trans_df1.interpolate(method = 'linear', limit_direction='forward')
            this_trans_df3 = this_trans_df2.interpolate(axis=0).where(this_trans_df1.bfill(axis=0).notnull())
                                                                      
           
            axis_name = this_trans_df3.plot(ax=ax1, kind=VISUAL_GRAPH_KIND, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color=VISUAL_COLOR_LIST[key])
            
            print('this_trans_df3',this_trans_df3)
            
            # now annotate this to be where the last not null value is
            
            lastvalue = this_trans_df3[pd.notnull(this_trans_df3)].idxmax()
            print('lastvalue', lastvalue)
            last_lastvalue = lastvalue[0]
            print('lastlastvalue',last_lastvalue)
            xvalue = this_trans_df3[last_lastvalue]
            #xvalue = this_trans_df3.index.get_loc(this_trans_df3[lastvalue].argmax())
            print('xvalue',xvalue)
            yvalue = new_price
            print('yvalue',yvalue)
            textxvalue = xvalue - 0.15
            textyvalue = yvalue 
            axis_name.annotate(full_tran_index_val, 
                               xy=(xvalue, yvalue), xycoords='data',  
                               xytext=(textxvalue, textyvalue), textcoords='data',
                               horizontalalignment='right', verticalalignment='bottom', 
                               rotation=(-1*VISUAL_XAXIS_ROTATION),
                               color = '#ff66ff') 
                                        
    #################
   
    # way do plotting depends on how many plots handling at once - one or many
    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0, box.width * VISUAL_LEGEND_BBOX_WIDTH, box.height])
    ax1.legend(loc='center left', bbox_to_anchor=VISUAL_LEGEND_BBOX_TO_ANCHOR, ncol=VISUAL_LEGEND_COLS, fancybox=VISUAL_LEGEND_FANCYBOX, fontsize=VISUAL_LEGEND_FONT_SIZE)
    
    plt.xticks(rotation=VISUAL_XAXIS_ROTATION)
            
    # now save the graphs to the specified pathname
    plt.savefig(output_name)

    #plt.show()
    
    
    
            
    return

      
if __name__ == "__main__":
    
    print("Started evaluate_volatility_strategy")
    print(" ")
    print("Open Database")
    print("-------------------------------")
     
    open_db("ims-invest", "ims_invest")
    
    
    # turn off or on the graphing part
    visualize = True
    
    start_date = '2017-01-06 00:00:00'
    end_date = '2017-01-07 16:00:00'
        
    # set up values of variables used for analysis
        
    algorithm_name = 'VOLATILITY1'
    
    if algorithm_name == 'VOLATILITY1':
        
        freq_type = '1 min'
        output_name = "/Users/moya/Documents/VOLATILITY1.png"
        override_config_dict = {}
        
        # setting up the override config dict - which will override the default values if a value is specified
        
        inv_ticker = 'CM'
        inv_exc_symbol = 'TSE'
        inv_ticker_exc_symbol = 'CM.TO'
        units_to_go_long = None
        units_to_go_short = None
        threshold_price_increase = None
        threshold_price_drop = None
    
        new_dict = {inv_ticker_exc_symbol :  [inv_ticker,
                                              inv_exc_symbol, 
                                              units_to_go_long, 
                                              units_to_go_short,
                                              threshold_price_increase,
                                              threshold_price_drop]}
        override_config_dict.update(new_dict)     
        
          
        results_dict = {}
        results_dict = evaluate_strategy(algorithm_name, start_date, end_date, freq_type, output_name, override_config_dict, visualize)
        
        # unpack and print out results
        print("Results of Evaluation for ", algorithm_name)
        
        for key in results_dict:
        
            values = results_dict[key]
            inv_ticker = values[0]
            inv_exc_symbol = values[1]
            number_opportunities = values[2]
            total_profit_loss = values[3]
            
            print("%s %s - Number of Opportunities %3.0f, Profit and loss $%5.2f " % (inv_ticker, inv_exc_symbol, number_opportunities, total_profit_loss))
        
        print("========================================")
    
    ####################################      
       
    #algorithm_name = 'VOLATILITY2'
    '''    
    if algorithm_name == 'VOLATILITY2':
        
        freq_type = '1 min'
        output_name = "/Users/moya/Documents/VOLATILITY2.png"
        
        override_config_dict = {}
        
        inv_ticker = 'CM'
        inv_exc_symbol = 'TSE'
        inv_ticker_exc_symbol = 'CM.TO'
        units_to_go_long = None
        units_to_go_short = None
        threshold_price_increase = None
        threshold_price_drop = None
    
        new_dict = {inv_ticker_exc_symbol :  [inv_ticker,
                                              inv_exc_symbol, 
                                              units_to_go_long, 
                                              units_to_go_short,
                                              threshold_price_increase,
                                              threshold_price_drop]}
        override_config_dict.update(new_dict)     
        
        results_dict = {}
        results_dict = evaluate_strategy(algorithm_name, start_date, end_date, freq_type, output_name, override_config_dict, visualize)
    
        # unpack and print out results
        print("Results of Evaluation for ", algorithm_name)
        
        for key in results_dict:
        
            values = results_dict[key]
            inv_ticker = values[0]
            inv_exc_symbol = values[1]
            number_opportunities = values[2]
            total_profit_loss = values[3]
            
            print("%s %s - Number of Opportunities %3.0f, Profit and loss $%5.2f " % (inv_ticker, inv_exc_symbol, number_opportunities, total_profit_loss))
        
        print("========================================")
    '''
    
    ####################################

    #algorithm_name = 'VOLATILITY3'
    '''
    if algorithm_name == 'VOLATILITY3':
        
        freq_type = '1 min'
        output_name = "/Users/moya/Documents/VOLATILITY3.png"
        
        override_config_dict = {}
        
        inv_ticker = 'CM'
        inv_exc_symbol = 'TSE'
        inv_ticker_exc_symbol = 'CM.TO'
        units_to_go_long = None
        units_to_go_short = None
        threshold_price_increase = None
        threshold_price_drop = None
    
        new_dict = {inv_ticker_exc_symbol :  [inv_ticker,
                                              inv_exc_symbol, 
                                              units_to_go_long, 
                                              units_to_go_short,
                                              threshold_price_increase,
                                              threshold_price_drop]}
        override_config_dict.update(new_dict)     
        
        results_dict = {}
        results_dict = evaluate_strategy(algorithm_name, start_date, end_date, freq_type, output_name, override_config_dict, visualize)
    
        ####################################
        
    # unpack and print out results
    print("Results of Evaluation for ", algorithm_name)
    
    for key in results_dict:
    
        values = results_dict[key]
        inv_ticker = values[0]
        inv_exc_symbol = values[1]
        number_opportunities = values[2]
        total_profit_loss = values[3]
        
        print("%s %s - Number of Opportunities %3.0f, Profit and loss $%5.2f " % (inv_ticker, inv_exc_symbol, number_opportunities, total_profit_loss))
    
    print("========================================")
    '''
            
        
    print("Close database")
    close_db()   
    
    print("Finished evaluate_volatility_strategy") 
