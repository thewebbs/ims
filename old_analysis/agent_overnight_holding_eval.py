#------------------------------------------------------------
# Filename  : agent_overnight_holding_eval.py
# Project   : ava
#
# Descr     : This holds routine to evaluate overnight holding profitability
#
# Params    : None
#
# History   :
#
# Date       ver Who  Change
# ---------- --- --- ------
# 2019-09-24   1 MW  Initial write
# ...
# 2021-12-19 100 DW  Added version 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------


from utils.config import DEBUG, REPORT_FOLDER_OSX
from datetime import date, datetime
from database.db_objects.ImsHistMktDataDB import get_ask_price_this_ticker_datetime, get_max_bid_price_in_date_range_for_ticker
from database.db_objects.ImsInvestmentDB import get_all_loading_investments
from database.db_objects.ImsOvrnghtHldgDetailDB import delete_these_ovrnght_hldg_details, ImsOvrnghtHldgDetailDB
from database.db_objects.ImsOvrnghtHldgSummarieDB import get_ovrnght_hldg_summary_id, ImsOvrnghtHldgSummarieDB
from decimal import Decimal
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from utils.utils_database import close_db, open_db
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_dates import daterange, next_business_date


def agent_overnight_holding_eval(database, inv_ticker, earliest_purchase_date, latest_purchase_date, time_for_sale, display_to_screen, days_to_hold_list,threshold_list, purchase_time_list, float_purchase_time_list ):
    
    results1_df = pd.DataFrame()
    results2_df = pd.DataFrame()
    results3_df = pd.DataFrame()

    start_date_as_string       = earliest_purchase_date.strftime("%Y-%m-%d") 
    end_date_as_string         = latest_purchase_date.strftime("%Y-%m-%d")
    current_datetime_as_string = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
     
    ohs_id = record_summary_in_database(database       = database,
                                        ohs_inv_ticker = inv_ticker, 
                                        ohs_start_date = start_date_as_string, 
                                        ohs_end_date   = latest_purchase_date, 
                                        ohd_run_date   = current_datetime_as_string)
    
    for number_of_days in days_to_hold_list:
        for profit_threshold in threshold_list:
            for time_for_purchase in purchase_time_list:

                trading_day, total_max_profit, total_threshold_profit, target_reached, this_record1, this_record2, this_record3 = \
                    calculate_this_holding_profit(database                   = database,              
                                                  ohs_id                     = ohs_id,
                                                  inv_ticker                 = inv_ticker, 
                                                  earliest_purchase_date     = earliest_purchase_date, 
                                                  latest_purchase_date       = latest_purchase_date, 
                                                  time_for_purchase          = time_for_purchase, 
                                                  time_for_sale              = time_for_sale, 
                                                  number_of_days             = number_of_days, 
                                                  profit_threshold           = profit_threshold,
                                                  start_date_as_string       = start_date_as_string,
                                                  end_date_as_string         = end_date_as_string,
                                                  current_datetime_as_string = current_datetime_as_string)

                # build the results into dataframes for visualization
                
                temp1_df    = pd.DataFrame(this_record1, columns = ['number_of_days', 'time_for_purchase', 'total_threshold_profit'])
                results1_df = pd.concat([results1_df, temp1_df])
                
                temp2_df    = pd.DataFrame(this_record2, columns = ['number_of_days', 'profit_threshold', 'total_threshold_profit'])
                results2_df = pd.concat([results2_df, temp2_df])
                
                temp3_df    = pd.DataFrame(this_record3, columns = ['time_for_purchase', 'profit_threshold', 'total_threshold_profit'])
                results3_df = pd.concat([results3_df, temp3_df])

    # only continue if data returned
    
    if len(results1_df.index) > 0:
    
        fig = plt.figure(figsize=(9,11))
        fig.suptitle("Overnight Holding Evaluation - Selling At Threshold", fontsize=14)
        plt.tight_layout()
        title2 = "%s between %s and %s" % (inv_ticker, earliest_purchase_date, latest_purchase_date)
        use_cmap = "YlOrRd"
                            
        # have a separate subplot for each combination
        ax1 = fig.add_subplot('311')
        ax1.set_title(title2, color='grey')
        if len(results2_df.index) > 0:
            produce_heatmap(results_df      = results2_df, 
                            groupby_column1 = 'profit_threshold', 
                            groupby_column2 = 'number_of_days', 
                            colorbar_label  = "Profit from selling at threshold", 
                            xlabel          = 'number_of_days', 
                            ylabel          = 'profit_threshold',
                            xticks          = days_to_hold_list,
                            yticks          = threshold_list,
                            cmap            = use_cmap,
                            axis            = ax1)
        
        ax2 = fig.add_subplot('312')
        if len(results3_df.index) > 0:
            produce_heatmap(results_df      = results3_df, 
                            groupby_column1 = 'profit_threshold', 
                            groupby_column2 = 'time_for_purchase', 
                            colorbar_label  = "Profit from selling at threshold", 
                            xlabel          = 'time_for_purchase',
                            ylabel          = 'profit_threshold', 
                            xticks          = purchase_time_list,
                            yticks          = threshold_list,
                            cmap            = use_cmap,
                            axis            = ax2)
    
        ax3 = fig.add_subplot('313')
        if len(results3_df.index) > 0:
            produce_heatmap(results_df      = results1_df, 
                            groupby_column1 = 'number_of_days', 
                            groupby_column2 = 'time_for_purchase', 
                            colorbar_label  = "Profit from selling at threshold", 
                            xlabel          = 'time_for_purchase',
                            ylabel          = 'number_of_days', 
                            xticks          = purchase_time_list,
                            yticks          = days_to_hold_list,
                            cmap            = use_cmap,
                            axis            = ax3)
        
        plt.subplots_adjust(left=None, bottom=0.1, right=None, top=0.9, wspace=0.2, hspace=0.2)
        
        # only display to the screen if parameter set
        if display_to_screen:
            plt.show()
        else:
            output_file_name = "%sOVERNIGHT_HOLD_%s_%s_%s_%s.pdf" % \
                    ( REPORT_FOLDER_OSX, inv_ticker, start_date_as_string, end_date_as_string, current_datetime_as_string)
            print('Saving output to ',output_file_name)
            plt.savefig(output_file_name)
            plt.close(fig)

    return total_max_profit, total_threshold_profit


def calculate_this_holding_profit(database, ohs_id, inv_ticker, earliest_purchase_date, latest_purchase_date, time_for_purchase, time_for_sale, number_of_days, profit_threshold, start_date_as_string, end_date_as_string, current_datetime_as_string):

    total_max_profit = 0
    total_threshold_profit = 0

    for single_date in daterange(earliest_purchase_date, latest_purchase_date):

            purchase_datetime_as_string = single_date.strftime("%Y-%m-%d") + time_for_purchase

            end_date = next_business_date(start_date           = single_date,
                                          business_days_to_add = number_of_days)

            last_sale_datetime_as_string = end_date.strftime("%Y-%m-%d") + time_for_sale

            trading_day, profit, target_reached = evaluate_this_combination(database                     = database,
                                                                            inv_ticker                   = inv_ticker,
                                                                            purchase_datetime_as_string  = purchase_datetime_as_string,
                                                                            last_sale_datetime_as_string = last_sale_datetime_as_string,
                                                                            profit_threshold             = profit_threshold)

            if profit == None:
                profit = 0
                
            total_max_profit += profit
            
            if target_reached:
                total_threshold_profit += float(profit_threshold)  # profit
            else:
                total_threshold_profit += float(profit)  # loss or small profit

    # first convert the time_for_purchase into something that could be a float
    float_time_for_purchase = float(time_for_purchase.replace(':','.',1))
    
    float_profit = float(total_threshold_profit)
    
    this_record1 = [(number_of_days, float_time_for_purchase, float_profit)]
    this_record2 = [(number_of_days, profit_threshold, float_profit)]
    this_record3 = [(float_time_for_purchase, profit_threshold, float_profit)]
    
    #print('before record_detail_in_database, ohs_id = ',ohs_id)
    record_detail_in_database(database              = database,
                              ohd_ohs_id            = ohs_id, 
                              ohd_number_days       = number_of_days, 
                              ohd_purchase_time     = time_for_purchase, 
                              ohd_profit_threshold  = profit_threshold, 
                              ohd_profit            = float_profit)

    return trading_day, total_max_profit, total_threshold_profit, target_reached, this_record1, this_record2, this_record3


def evaluate_this_combination(database, inv_ticker, purchase_datetime_as_string, last_sale_datetime_as_string, profit_threshold):

    purchase_price = get_ask_price_this_ticker_datetime(database           = database,
                                                        hmd_inv_ticker     = inv_ticker,
                                                        hmd_start_datetime = purchase_datetime_as_string)

    if purchase_price == None:
        purchase_price = Decimal(0)

    if purchase_price > 0:
        trading_day = True

        sales_price = get_max_bid_price_in_date_range_for_ticker(database           = database,
                                                                 hmd_inv_ticker     = inv_ticker,
                                                                 hmd_start_datetime = purchase_datetime_as_string,
                                                                 hmd_end_datetime   = last_sale_datetime_as_string)
        if sales_price == None:
            sales_price = Decimal(0)

        profit = sales_price - purchase_price
        target_reached = (sales_price - purchase_price)>profit_threshold

    else:
        trading_day = False
        profit = 0
        target_reached = False

    return trading_day, profit, target_reached


def produce_heatmap(results_df, groupby_column1, groupby_column2, colorbar_label, xlabel, ylabel, xticks, yticks, cmap, axis):

    results_df = results_df.astype(float)
    graph_results_df = results_df.groupby([groupby_column1, groupby_column2], as_index=False).sum().pivot(groupby_column1, groupby_column2).fillna(0)
    
    print(graph_results_df)
    
    xticks_min = results_df[xlabel].min(axis=None, skipna=True)
    xticks_max = results_df[xlabel].max(axis=None, skipna=True)
    yticks_min = graph_results_df.index.max()
    yticks_max = graph_results_df.index.min()
    
    centers=[xticks_min, xticks_max, yticks_min, yticks_max]
    dx, = np.diff(centers[:2])/(graph_results_df.shape[1]-1)
    dy, = -np.diff(centers[2:])/(graph_results_df.shape[0]-1)
    extent=[centers[0]-dx/2, centers[1]+dx/2, centers[2]+dy/2, centers[3]-dy/2]
    
    plt.imshow(graph_results_df, cmap=cmap, interpolation=None, extent=extent, aspect='auto')
    plt.colorbar(label=colorbar_label)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(np.arange(centers[0], centers[1]+dx, dx))
    plt.yticks(np.arange(centers[3], centers[2]+dy, dy))
    
    # Add the text
    ysize = len(yticks)
    xsize = len(xticks)
    jump_x = (xticks_max - xticks_min) / (2.0 * xsize)
    jump_y = (yticks_min - yticks_max) / (2.0 * ysize)
    x_positions = np.linspace(start=xticks_min, stop=xticks_max, num=xsize, endpoint=False)
    y_positions = np.linspace(start=yticks_max, stop=yticks_min, num=ysize, endpoint=False)
    axis.autoscale(enable=True,axis='y')
    
    for y_index, y in enumerate(y_positions):
        for x_index, x in enumerate(x_positions):
            label = graph_results_df.values[y_index, x_index]
            formatted_label = "%.0f" % (label)
            text_x = x + jump_x
            text_y = y + jump_y
            axis.text(text_x, text_y, formatted_label, color='black', ha='center', va='center')
    
    return


def record_detail_in_database(database, ohd_ohs_id, ohd_number_days, ohd_purchase_time, ohd_profit_threshold, ohd_profit):
    
    this_ovrnght_hldg_summaries = ImsOvrnghtHldgDetailDB(database             = database,
                                                            ohd_ohs_id           = ohd_ohs_id, 
                                                            ohd_number_days      = ohd_number_days, 
                                                            ohd_purchase_time    = ohd_purchase_time, 
                                                            ohd_profit_threshold = ohd_profit_threshold, 
                                                            ohd_profit           = ohd_profit)
                           
    this_ovrnght_hldg_summaries.insert_DB()
    
    return 


def record_summary_in_database(database, ohs_inv_ticker, ohs_start_date, ohs_end_date, ohd_run_date):
    
    # work out if this summary record already exists and if so get the ohs_id
    
    ohs_ids = get_ovrnght_hldg_summary_id(database       = database,
                                          ohs_inv_ticker = ohs_inv_ticker,
                                          ohs_start_date = ohs_start_date, 
                                          ohs_end_date   = ohs_end_date)
    
    if ohs_ids == []:
        
        this_ods_id = ''
       
        this_ovrnght_hldg_summaries = ImsOvrnghtHldgSummarieDB(database       = database,
                                                               ohs_id         = this_ods_id, 
                                                               ohs_inv_ticker = ohs_inv_ticker, 
                                                               ohs_start_date = ohs_start_date, 
                                                               ohs_end_date   = ohs_end_date, 
                                                               ohs_run_date   = ohd_run_date)
        this_ovrnght_hldg_summaries.insert_DB()
        
        # now get it again
        ohs_ids = get_ovrnght_hldg_summary_id(database       = database,
                                              ohs_inv_ticker = ohs_inv_ticker,
                                              ohs_start_date = ohs_start_date, 
                                              ohs_end_date   = ohs_end_date)
    
        #print('sts_ids',sts_ids)
        for (this_ohs_id) in ohs_ids[0]:
            if DEBUG:
                print('this_ohs_id', this_ohs_id)
                
    else:
        
        # shouldn't really get here as sequence should have been newly generated but code here in case
        #print('does exist so pick it off')
        for (this_ohs_id) in ohs_ids[0]:
            if DEBUG:
                print('this_ohs_id', this_ohs_id)
       
        # need to delete the details in this case to be sure
        delete_these_ovrnght_hldg_details(database   = database,
                                          ohd_ohs_id = this_ohs_id)                 
                
    return this_ohs_id


if __name__ == "__main__":

    print("Open db")
    print(" ")
    
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)
    
    #
    # set up parameters
    #
    # the following are the combinations that we'll check out
    
    display_to_screen        = False
    days_to_hold_list        = [1,2,3,4]
    threshold_list           = [Decimal(0.25), Decimal(0.5), Decimal(0.75), Decimal(1)]
    purchase_time_list       = [' 07:00', ' 8:00', ' 09:00', ' 10:00', ' 11:00', ' 12:00' ]
    float_purchase_time_list = [ 7, 8, 9, 10, 11, 12]
    earliest_purchase_date   = date(2019, 8, 1)
    latest_purchase_date     = date(2019, 8, 31)
    time_for_sale            = ' 12:59:59'
    
    # if the inv_ticker passed in is % then loop through all
    #inv_ticker = 'CM.TO'
    
    inv_ticker = '%'
    
    tickers = get_all_loading_investments(database   = database,
                                          inv_ticker = inv_ticker)
    
    if tickers != None:
        for ticker in tickers:
            this_inv_ticker = ticker[0]

            total_max_profit, total_threshold_profit = agent_overnight_holding_eval(database                 = database, 
                                                                                    inv_ticker               = this_inv_ticker, 
                                                                                    earliest_purchase_date   = earliest_purchase_date, 
                                                                                    latest_purchase_date     = latest_purchase_date, 
                                                                                    time_for_sale            = time_for_sale, 
                                                                                    display_to_screen        = display_to_screen,
                                                                                    days_to_hold_list        = days_to_hold_list,
                                                                                    threshold_list           = threshold_list,
                                                                                    purchase_time_list       = purchase_time_list,
                                                                                    float_purchase_time_list = float_purchase_time_list)
          
            print('total_max_profit', total_max_profit,'total_threshold_profit', total_threshold_profit)    
        
    print(" ")
    print("Close db")
    print(" ")
    
    close_db(database = database)


