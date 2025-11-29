#------------------------------------------------------------
# Filename  : find_trade_opps.py
# Project   : ava
#
# Descr     : This file contains code to find trading opportunities
#
# Params    : database
#             start_date
#             end_date
#             show_graphs
#             num_to_return
#             minprice
#             maxspread
#             printlegend
#             output_filename
#             graphtitle
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2016-04-14   1 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from decimal import Decimal
import matplotlib.pyplot as plt
from database.db_objects.ImsInvestmentDB import get_all_viable_trading_investments
from database.db_objects.ImsHistMktDataDB import get_last_bid_ask_price_in_range
from database.db_objects.ImsTradeOppsDB import ImsTradeOppsDB
import pandas as pd
from utils.config import DEBUG, MIN_TRADEOPP_SCALE, MIN_WIBBLINESS_THRESHOLD, REPORT_FOLDER_OSX
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import open_db, close_db 
from utils.utils_dataframes import format_changes_df

def find_trade_opps(database, start_date, end_date, show_graphs, num_to_return, minprice, maxspread, printlegend, output_filename, graphtitle ):

    total_df = check_all_tickers(database      = database,
                                 start_date    = start_date, 
                                 end_date      = end_date, 
                                 show_graphs   = show_graphs, 
                                 num_to_return = num_to_return, 
                                 minprice      = minprice, 
                                 maxspread     = maxspread)
    
    transposedtotal_df = total_df.transpose()
    print('Top tickers in order:')
    for resultrec in transposedtotal_df:
        this_ticker = resultrec
        print(this_ticker)

    save_results_to_table(database   = database, 
                          start_date = start_date, 
                          end_date   = end_date,
                          in_df      = total_df)
    
    graph_dfs(in_df           = transposedtotal_df, 
              ttitle          = graphtitle, 
              printlegend     = printlegend,
              show_graphs     = show_graphs,
              output_filename = output_filename)

    print(' ')
    
    return transposedtotal_df


def add_to_df(in_df, displaytype, inv_ticker, this_datetime, bid_price, directiontriggered):

    # note displaytype can be 'W' for whole or 'C' for changes
    
    if DEBUG:
        if displaytype == 'C':
            if directiontriggered != 'I' and directiontriggered != 'E':
                print(("price direction change %s for %s date %s adj_close %.2f ") % (directiontriggered, inv_ticker, this_datetime, bid_price))
    
    format_date = this_datetime.strftime('%y%m%d')
    temp = pd.DataFrame({format_date: (float(bid_price))},  
                             index=[displaytype] )
    
    in_df = pd.concat([in_df, temp], sort=True)
    
    return in_df


def check_all_tickers(database, start_date, end_date, show_graphs, num_to_return, minprice, maxspread):
    
    if DEBUG:
        print("checking all tickers between %s and %s and returning %.f" % (start_date, end_date, num_to_return))

    number_good_oppsup   = 0
    number_good_oppsdown = 0
    total_df             = pd.DataFrame()
    
    ticker_list = get_all_viable_trading_investments(database   = database,
                                                     inv_ticker = '%',
                                                     minprice = minprice,
                                                     maxspread = maxspread)
    
    for inv_ticker_tuple in ticker_list:
        inv_ticker = inv_ticker_tuple[0]
        
        number_good_oppsup, number_good_oppsdown = eval_trade_opps(database = database,
                                                                   inv_ticker = inv_ticker, 
                                                                   start_date = start_date, 
                                                                   end_date = end_date)
        
        
        temp       = pd.DataFrame({'Tickers' : (float(number_good_oppsup))+ (float(number_good_oppsdown))}, index=[inv_ticker] )
        total_df = pd.concat([total_df, temp], sort=True)
        
        save_results_to_table(database   = database, 
                      start_date = start_date, 
                      end_date   = end_date,
                      in_df      = total_df)
        
    sortedtotal_df = total_df.sort_values(total_df.columns[0], axis=0, ascending=False, kind='quicksort')
    
    returntotal_df = sortedtotal_df.iloc[:num_to_return:]
    
    
    if DEBUG:
        print(returntotal_df)
                 
    return returntotal_df


def eval_trade_opps(database, inv_ticker, start_date, end_date):
     
    # initialize tracking variables 
    numups = 0
    numdowns = 0
    numflats = 0
    numrecs = 0
    current_direction = ''
    latest_direction = ''

    value_at_last_dir_change = 0
    date_at_last_dir_change = ''
    percent_change = 0
        
    cum_diffs_last_3 = 0
    cum_diffs_last_5 = 0
    
    five_diffs_ago = 0
    five_dates_ago = ''
    five_values_ago = 0
    
    four_diffs_ago = 0
    four_dates_ago = ''
    four_values_ago = 0
    
    three_diffs_ago = 0
    three_dates_ago = ''
    three_values_ago = 0
    
    two_diffs_ago = 0
    two_dates_ago = ''
    two_values_ago = 0
    
    one_diff_ago = 0
    one_date_ago = ''
    one_value_ago = 0
    
    last_direction_triggered = ''
    
    whole_df = pd.DataFrame()      # for all records
    changes_df = pd.DataFrame()    # just for the changes 
        
    prices = get_last_bid_ask_price_in_range(database = database, 
                                             hmd_inv_ticker = inv_ticker, 
                                             hmd_start_datetime = start_date, 
                                             hmd_end_datetime = end_date, 
                                             hmd_freq_type = '1 min')
    if DEBUG:
        print(prices)

    last_bid_price = 0
    last_datetime = ''
    this_datetime = ''
    bid_price = 0
    
    for (this_datetime, bid_price, ask_price) in prices:
    
        if bid_price == None:
            bid_price = Decimal(0)

        if ask_price == None:
            ask_price = Decimal(0)

        numrecs += 1
        whole_df = add_to_df(whole_df, 'W', inv_ticker, this_datetime, bid_price,'W') 
        
        if last_bid_price == 0:
            last_bid_price = bid_price
        
        if last_datetime == '':
            last_datetime = this_datetime
                           
        this_diff = bid_price - last_bid_price 
        if this_diff == None:
            this_diff = Decimal(0)
        
        if last_bid_price >0:
            percent_change = 100*(this_diff/last_bid_price)
        else:
            percent_change = Decimal(0)
            
        # work out initial direction
        if current_direction == '':
            if this_diff > 0:
                current_direction = 'U'
                last_direction_triggered = 'U'
                
                if DEBUG:
                    print("initial direction up")
 
            elif this_diff < 0:
                current_direction = 'D'
                last_direction_triggered = 'D'
                
                if DEBUG:
                    print("initial direction down")

        if value_at_last_dir_change == 0:
            value_at_last_dir_change = bid_price
            date_at_last_dir_change = this_datetime
    
            if DEBUG:
                print(('value at first direction change %.2f date at first direction change %s') % (value_at_last_dir_change, date_at_last_dir_change))
                
            changes_df = add_to_df(changes_df, 'C', inv_ticker, date_at_last_dir_change, value_at_last_dir_change,'I' )

                       
        # what direction is this in 
        if this_diff > 0:
            latest_direction = 'U'
            if percent_change > MIN_WIBBLINESS_THRESHOLD:
            
                if current_direction != latest_direction:
                    
                    # check we're not triggering twice
                    if date_at_last_dir_change < last_datetime:
                        
                        numups += 1
                    
                        value_at_last_dir_change = last_bid_price
                        date_at_last_dir_change = last_datetime
        
                        if DEBUG:
                            print(('change in direction - was %s and now %s - date %s this_value %.2f value at last change %.2f ') % (current_direction, latest_direction, date_at_last_dir_change, bid_price, value_at_last_dir_change))
                        
                        last_direction_triggered = 'U'        
                        changes_df = add_to_df(changes_df, 'C', inv_ticker, date_at_last_dir_change, value_at_last_dir_change, last_direction_triggered)
                        current_direction = latest_direction

        elif this_diff < 0:
            latest_direction = 'D'
            if percent_change < (-1 * MIN_WIBBLINESS_THRESHOLD):
            
                if current_direction != latest_direction:
                    
                    # check we're not triggering twice
                    if date_at_last_dir_change < last_datetime:
                        
                        numdowns += 1
                        value_at_last_dir_change = last_bid_price
                        date_at_last_dir_change = last_datetime
                    
                        if DEBUG:
                            print(('change in direction - was %s and now %s - date %s this_value %.2f value at last change %.2f ') % (current_direction, latest_direction, date_at_last_dir_change, bid_price, value_at_last_dir_change))
                        
                        last_direction_triggered = 'D'          
                        changes_df = add_to_df(changes_df, 'C', inv_ticker, date_at_last_dir_change, value_at_last_dir_change, last_direction_triggered)
                        current_direction = latest_direction

        else:
            latest_direction = 'N'
            current_direction = 'N'
        
        # roll the previous values for checking cumulative changes
        five_diffs_ago = four_diffs_ago      
        five_dates_ago = four_dates_ago      
        five_values_ago = four_values_ago
        
        four_diffs_ago = three_diffs_ago
        four_dates_ago = three_dates_ago
        four_values_ago = three_values_ago
        
        three_diffs_ago = two_diffs_ago      
        three_dates_ago = two_dates_ago      
        three_values_ago = two_values_ago
        
        two_diffs_ago = one_diff_ago
        two_dates_ago = one_date_ago
        two_values_ago = one_value_ago
        
        one_diff_ago = this_diff
        one_date_ago = this_datetime
        one_value_ago = bid_price
        
        last_bid_price = bid_price
        last_datetime = this_datetime
        
        # accumulate last 3 and last 6 to detect flatness and to detect cumulative changes in one direction
        
        cum_diffs_last_5 = five_diffs_ago + four_diffs_ago + three_diffs_ago + two_diffs_ago + one_diff_ago
        cum_diffs_last_3 = three_diffs_ago + two_diffs_ago + one_diff_ago
        
        
        # check on results of lots of small changes
        if current_direction == 'D' and cum_diffs_last_3 > MIN_WIBBLINESS_THRESHOLD:
            # check we're not triggering twice
                    
            if date_at_last_dir_change < last_datetime:            
                    
                numups += 1
                value_at_last_dir_change = last_bid_price
                date_at_last_dir_change = last_datetime
        
                if DEBUG:
                    print(('   cumulative change in direction - was %s and now %s - date %s this_value %.2f value at last change %.2f ') % (current_direction, latest_direction, date_at_last_dir_change, bid_price, value_at_last_dir_change))
                
                last_direction_triggered = 'U'    
                changes_df = add_to_df(changes_df, 'C', inv_ticker, date_at_last_dir_change, value_at_last_dir_change, last_direction_triggered)
                current_direction = 'U'
        

        elif current_direction == 'U' and cum_diffs_last_3 < (-1*MIN_WIBBLINESS_THRESHOLD):
            # check we're not triggering twice
                    
            if date_at_last_dir_change < last_datetime:
                        
                numdowns += 1
                value_at_last_dir_change = last_bid_price
                date_at_last_dir_change = last_datetime
                    
                if DEBUG:
                    print(('   cumulative change in direction - was %s and now %s - date %s this_value %.2f value at last change %.2f ') % (current_direction, latest_direction, date_at_last_dir_change, bid_price, value_at_last_dir_change))
        
                last_direction_triggered = 'D'
                changes_df = add_to_df(changes_df, 'C', inv_ticker, date_at_last_dir_change, value_at_last_dir_change, last_direction_triggered)
                current_direction = 'D'                
        
        if DEBUG:
            print(("this_diff %.2f and percent_change %.2f pri_adj_close %.2f pri_date %s  #Up opps %.f and #Down opps %.f current_direction %s latest_direction %s  cum_diffs_last_3 %.2f") \
                  % (this_diff, percent_change, bid_price, this_datetime, numups,numdowns, current_direction, latest_direction, cum_diffs_last_3))
            print(" ")
            
        # detect flatness
        
        if five_diffs_ago != 0:
            if cum_diffs_last_5 < (0.25*MIN_WIBBLINESS_THRESHOLD):
                if cum_diffs_last_5 > (-0.25 * MIN_WIBBLINESS_THRESHOLD):
                    
                    # check we haven't just triggered elsewhere:
                    if (current_direction == 'D' and cum_diffs_last_5 <= (0.5*MIN_WIBBLINESS_THRESHOLD)):
                                          
                        latest_direction = 'F'

                        # just became flat
                        if (current_direction != latest_direction) and (last_direction_triggered != 'F'): 
                    
                            last_direction_triggered = 'F'
                            
                            # now if the last direction triggered was before the date that six dates ago then use it else don't
                            if (date_at_last_dir_change < five_dates_ago):
                            
                                changes_df = add_to_df(changes_df, 'C', inv_ticker, five_dates_ago, five_values_ago, last_direction_triggered)
                                
                                # now need to stop it being triggered again too quickly
                                five_diffs_ago = 0
                                four_diffs_ago= 0
                                three_diffs_ago = 0
                                two_diffs_ago = 0
                            

                    if (current_direction == 'U' and cum_diffs_last_5 > (-0.5*MIN_WIBBLINESS_THRESHOLD)):
                                          
                        latest_direction = 'F'
                        
                        # just became flat
                        if (current_direction != latest_direction) and (last_direction_triggered != 'F'):
                    
                            last_direction_triggered = 'F'
                            numflats += 1
                    
                            # now if the last direction triggered was before the date that six dates ago then use it else don't
                            if (date_at_last_dir_change < five_dates_ago):
                                changes_df = add_to_df(changes_df, 'C', inv_ticker, five_dates_ago, five_values_ago, last_direction_triggered)
                            
                                # now need to stop it being triggered again too quickly
                                five_diffs_ago = 0
                                four_diffs_ago= 0
                                three_diffs_ago = 0
                                two_diffs_ago = 0
                                        
    
    if date_at_last_dir_change != this_datetime:     
        
        if DEBUG:
            print(('at the very end direction was %s - date %s this_value %.2f ') % (latest_direction, this_datetime, bid_price))
            print(('about to call add_to_df last_direction_triggered %s') % (last_direction_triggered))
        
        changes_df = add_to_df(changes_df, 'C', inv_ticker, this_datetime, bid_price, 'E')                
        
    if DEBUG:
        if (numups + numdowns) > 0:
            print(('Summary for %s - Up opportunities %.f - Down opportunities %.f - Total opportunities %.f') % (inv_ticker, numups, numdowns, numups + numdowns))
           
    changes_df = changes_df.groupby(changes_df.index).sum()    
    
    if DEBUG:
        print(changes_df)
    
    whole_df = whole_df.groupby(whole_df.index).sum()    
    
    if DEBUG:
        print(whole_df)
    
    # join the dataframes together
    frames = [whole_df, changes_df]
    out_df = pd.concat(frames, sort=True)
    out_df = out_df.transpose()
    
    # create a dataframe containing just the changes and work out the scale changes
    
    changes_df = pd.DataFrame(out_df, columns = list('C'))
    changes_df = changes_df.dropna(axis=0)
    combined_df = format_changes_df(changes_df)
    
    # now look at the scalechanges to see how many of them exceed the MIN_TRADEOPP_SCALE
    good_opps_up_df = combined_df.loc[combined_df["scalechange"] > MIN_TRADEOPP_SCALE]
    good_opps_down_df = combined_df.loc[combined_df["scalechange"] < (-1*MIN_TRADEOPP_SCALE)]
    numgoodoppsup = len(good_opps_up_df.index)
    numgoodoppsdown = len(good_opps_down_df.index)
    
    return (numgoodoppsup, numgoodoppsdown)


def graph_dfs(in_df, ttitle, printlegend, show_graphs, output_filename):
        
    in_df = in_df.interpolate()
    
    ax1 = in_df.plot.bar(title = ttitle, rot=0, sharex=True, sharey=True)
    
    if printlegend:
        # legend to right hand side outside of box
        box = ax1.get_position()
        ax1.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        ax1.legend(loc='center left', bbox_to_anchor=(1, 0.5), ncol=1, fancybox=True, fontsize='small')
   
    if show_graphs:
        plt.show()
    
    plt.savefig(output_filename)

    return

def save_results_to_table(database, start_date, end_date, in_df):
    
    
    for inv_ticker, df_row in in_df.iterrows():
        
        num_good_trade_opps = df_row['Tickers']
        
        new_ims_trade_opps = ImsTradeOppsDB(database          = database,
                                            top_inv_ticker    = inv_ticker,
                                            top_start_date    = start_date, 
                                            top_end_date      = end_date, 
                                            top_num_good_opps = num_good_trade_opps)
        
        new_ims_trade_opps.insert_DB()
    
    
    return 
    
if __name__ == "__main__":
    
    print("Started find_trade_opps")
    print(" ")
    print("Open Database")
        
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)   
    
    show_graphs    = False
    printlegend    = True
    num_to_return   = 10
    minprice        = 50
    maxspread       = 0.1
    
    # looping through producing a list and a graph for each month
    start_date_list = ['2019-01-01','2019-02-01','2019-03-01','2019-04-01','2019-05-01','2019-06-01','2019-07-01','2019-08-01','2019-09-01','2019-10-01','2019-11-01','2019-12-01']
    end_date_list   = ['2019-01-31','2019-02-28','2019-03-31','2019-04-30','2019-05-31','2019-06-30','2019-07-31','2019-08-31','2019-09-30','2019-10-31','2019-11-30','2019-12-31']
    
    for counter in range(0,12):
        start_date = start_date_list[counter]
        end_date   = end_date_list[counter]
    
        output_filename = "%sTop_Trading_tickers_from_%s_to_%s.png" % (REPORT_FOLDER_OSX, start_date, end_date )  
        graphtitle      = "Top Trading Tickers from %s to %s" % (start_date, end_date)
        
        print('+++++++++++++++++++++++')
        print('Searching for ',start_date,'TSE',end_date)
        transposedtotal_df = find_trade_opps(database, start_date, end_date, show_graphs, num_to_return, minprice, maxspread, printlegend, output_filename, graphtitle)

                     
    print("Close database")
    close_db(database = database)   
    
    print("Finished find_trade_opps") 
    
    
 
