#------------------------------------------------------------
# Filename  : StreakProcessing.py
# Project   : ava
#
# Descr     : This file contains code to process streaks in historical
#             market data. This is based on the old DetectStreaks but
#             is now a class of it's own
#
# Params    : None
#
# History  :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2019-09-10   1 MW  Initial Write
# 2021-08-25 100 DW  Added version and moved to ILS-ava
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from utils.config import DEBUG, STREAK_LOOKBACK_PERIOD, STREAK_LOOKBACK_THRESHOLD, VISUAL_COLOR_LIST, VISUAL_GRAPH_KIND, VISUAL_LEGEND_BBOX_TO_ANCHOR
from utils.config import VISUAL_LEGEND_BBOX_WIDTH, VISUAL_LEGEND_COLS, VISUAL_LEGEND_FANCYBOX, VISUAL_LEGEND_FONT_SIZE, VISUAL_GREY_LINE_COLOR
from utils.config import VISUAL_LINE_STYLE, VISUAL_LINE_WEIGHT, VISUAL_STYLE_TO_USE, VISUAL_TOP_TITLE_FONT_SIZE
from utils.config import VISUAL_2ND_TITLE_FONT_SIZE, VISUAL_XAXIS_COLOR, VISUAL_XAXIS_ROTATION, STREAK_THRESHOLD_PERCENTAGE

from decimal import Decimal
import matplotlib.pyplot as plt
import pandas as pd
from database.db_objects.ImsHistMktDataDB import get_first_hist_mkt_data_in_range, get_hist_mkt_data_in_range
from database.db_objects.ImsStreakDetailDB import ImsStreakDetailDB, delete_these_streak_details
from database.db_objects.ImsStreakSummarieDB import ImsStreakSummarieDB, delete_these_streak_summaries, get_streak_summary_id
from utils.utils_dataframes import fixit_df


#
# Class definition
#

class StreakProcessing:

    def __init__(self, database, inv_ticker, start_date, end_date, produce_graph, display_to_screen, output_path_name, number_ups, number_downs, number_flats, profit, freq_type):

        self.database          = database
        self.inv_ticker        = inv_ticker
        self.start_date        = start_date
        self.end_date          = end_date
        self.produce_graph     = produce_graph
        self.display_to_screen = display_to_screen
        self.output_path_name  = output_path_name
        self.number_ups        = number_ups
        self.number_downs      = number_downs
        self.number_flats      = number_flats
        self.profit            = profit
        self.freq_type         = freq_type
        
        return 
    

    def delete_existing_streaks(self):
    
        #
        # because the key sequence is auto increment we set it to null here
        #
        
        these_sts_ids    = []
        these_sts_ids[:] = get_streak_summary_id(database      = self.database,
                                                sts_inv_ticker = self.inv_ticker,
                                                start_date     = self.start_date,
                                                end_date       = self.end_date,
                                                sts_threshold  = STREAK_LOOKBACK_THRESHOLD)
    
        #
        # now loop through the these_sts_id as there will be many and delete them
        #
        
        for this_sts_id in these_sts_ids:
    
            # first delete the details
            
            result = delete_these_streak_details(database        = self.database,
                                                 std_sts_id      = this_sts_id,
                                                 start_datetime  = self.start_date,
                                                 end_datetime    = self.end_date,
                                                 std_streak_type = '%')
            
            if DEBUG:
                print('result after delete_these_streak_details ', result)
    
            # then delete the summary itself
            
            result = delete_these_streak_summaries(database       = self.database,
                                                   sts_inv_ticker = self.inv_ticker,
                                                   start_date     = self.start_date,
                                                   end_date       = self.end_date,
                                                   sts_threshold  = STREAK_LOOKBACK_THRESHOLD)
            
            if DEBUG:
                print('result after delete_these_streak_details ', result)
    
        return


    def detectstreaks(self):

        self.number_ups   = 0
        self.number_downs = 0
        self.number_flats = 0
        self.profit       = 0
        
        print('=======================================================================')
        print('detect streaks - processing for ', self.inv_ticker, 'between ', self.start_date, 'and', self.end_date)

        # first delete any existing

        self.delete_existing_streaks()


        # now start looking for streaks
        
        current_index      = 1
        streak_start_index = current_index
        current_direction  = 'flat'
        streaks_df         = pd.DataFrame()


        # if the streak threshold percentage has a value then calculate the lookback_threshold
        # based on the threshold percentage and the first price for this ticker
        
        if STREAK_THRESHOLD_PERCENTAGE != 0:
            first_price = self.first_price_in_range()[0]
            if first_price == None:
                first_price = 0
                
            if first_price != 0:
                lookback_threshold = STREAK_THRESHOLD_PERCENTAGE/100 * first_price
            else:
                lookback_threshold    = STREAK_LOOKBACK_THRESHOLD
        else:
            lookback_threshold    = STREAK_LOOKBACK_THRESHOLD 
        
        # build the lists of the prices and dates and also a dataframe ready for graphing
        
        bid_price_list, ask_price_list, midpoint_price_list, date_list, prices_df = self.get_price_lists()
        number_recs = len(midpoint_price_list)

        # only proceed if data was returned

        if number_recs == 0:
            print('No records returned for this ticker, date range')
            
            self.number_ups   = 0
            self.number_downs = 0
            self.number_flats = 0
            self.profit  = 0

        else:

            for current_index in range(number_recs):
                next_direction = self.get_direction(price_list         = midpoint_price_list, 
                                                    current_index      = current_index, 
                                                    current_direction  = current_direction)
                
                if next_direction != current_direction:
                    
                    if self.streak_has_ended(price_list         = midpoint_price_list, 
                                             current_index      = current_index, 
                                             current_direction  = current_direction, 
                                             lookback_period    = STREAK_LOOKBACK_PERIOD, 
                                             lookback_threshold = lookback_threshold 
                                             ):

                        # Note that need to look at the record immediately before the Current_index as the end of the streak
                        # not the Current_index

                        streak_end_index = current_index - 1

                        # for calculating profit need to subtract the start_bid_price from the end_ask_price if it's an up and vice versa if it's down so need to capture all
                        
                        start_bid_price = bid_price_list[streak_start_index]
                        start_ask_price = ask_price_list[streak_start_index]
                        end_bid_price = bid_price_list[streak_end_index]
                        end_ask_price = ask_price_list[streak_end_index]
                        start_midpoint_price = midpoint_price_list[streak_start_index]
                        end_midpoint_price   = midpoint_price_list[streak_end_index]
                        start_datetime       = date_list[streak_start_index]
                        end_datetime         = date_list[streak_end_index]
                        streaks_df           = self.record_streak(start_index          = streak_start_index, 
                                                                  end_index            = streak_end_index, 
                                                                  start_bid_price      = start_bid_price, 
                                                                  start_ask_price      = start_ask_price, 
                                                                  end_bid_price        = end_bid_price, 
                                                                  end_ask_price        = end_ask_price, 
                                                                  start_midpoint_price = start_midpoint_price, 
                                                                  end_midpoint_price   = end_midpoint_price, 
                                                                  start_datetime       = start_datetime, 
                                                                  end_datetime         = end_datetime, 
                                                                  direction            = current_direction, 
                                                                  streaks_df           = streaks_df)

                        current_direction  = next_direction
                        streak_start_index = current_index

            # Don't forget the last one
            # for calculating profit need to subtract the start_bid_price from the end_ask_price (or vice versa if a down) so need to capture all
            
            start_bid_price      = bid_price_list[streak_start_index]
            start_ask_price      = ask_price_list[streak_start_index]
            end_bid_price        = bid_price_list[current_index]
            end_ask_price        = ask_price_list[current_index]
            start_midpoint_price = midpoint_price_list[streak_start_index]
            end_midpoint_price   = midpoint_price_list[current_index]
            start_datetime       = date_list[streak_start_index]
            end_datetime         = date_list[current_index]
            
            streaks_df = self.record_streak(start_index          = streak_start_index , 
                                            end_index            = current_index, 
                                            start_bid_price      = start_bid_price, 
                                            start_ask_price      = start_ask_price, 
                                            end_bid_price        = end_bid_price, 
                                            end_ask_price        = end_ask_price, 
                                            start_midpoint_price = start_midpoint_price, 
                                            end_midpoint_price   = end_midpoint_price, 
                                            start_datetime       = start_datetime, 
                                            end_datetime         = end_datetime, 
                                            direction            = current_direction, 
                                            streaks_df           = streaks_df)

            streaks_df = streaks_df.groupby(streaks_df.index).sum()

            if len(streaks_df.index) > 0:
                streaks_df = fixit_df(streaks_df)

            results = "Number Ups: %2.0f  Number Downs: %2.0f  Number Flats: %2.0f  Profit: $%2.2f" % (self.number_ups, self.number_downs, self.number_flats, self.profit)
            print(results)
            
            if self.produce_graph:
                results = self.graph_results(prices_df          = prices_df, 
                                             streaks_df         = streaks_df,
                                             lookback_threshold = lookback_threshold)

        return 


    def get_direction(self, price_list, current_index, current_direction):
    
        new_direction = current_direction
        
        if current_index > 1:
            # we will not go before start of datastructure
            
            previous_value = price_list[current_index-1]
            current_value  = price_list[current_index]
    
            if current_value > previous_value:
                new_direction = 'up'
            elif current_value < previous_value:
                new_direction = 'down'
            else:
                new_direction = 'flat'
        else:
            new_direction = 'flat'
    
        if DEBUG:
            if current_index > 1:
                print('In get_direction, current_direction = ',current_direction,' previous_value = ',previous_value,' Current_value = ',current_value, ' new_direction =', new_direction)
    
        return new_direction


    def first_price_in_range(self):
        
        hist_mkt_prices = []
        first_price = Decimal(0)
        
        hmd_inv_ticker = self.inv_ticker 
        hist_mkt_prices[:] = get_first_hist_mkt_data_in_range(database           = self.database,
                                                              hmd_inv_ticker     = hmd_inv_ticker,
                                                              hmd_start_datetime = self.start_date,
                                                              hmd_end_datetime   = self.end_date,
                                                              hmd_freq_type      = self.freq_type)
    
        for (first_price) in hist_mkt_prices:
    
            if first_price == None:
                first_price = Decimal(0)
    
        return first_price

        
    def get_price_lists(self):
    
        bid_price_list      = []
        ask_price_list      = []
        midpoint_price_list = []
        date_list           = []
    
        # get midpoint price data and build into price and date lists for processing
    
        hist_mkt_prices = []
        
        #freq_type = '1 min'
        prices_df = pd.DataFrame()
    
        # note that in the database inv_ticker includes the exchange symbol
    
        hmd_inv_ticker = self.inv_ticker 
        hist_mkt_prices[:] = get_hist_mkt_data_in_range(database           = self.database,
                                                        hmd_inv_ticker     = hmd_inv_ticker,
                                                        hmd_start_datetime = self.start_date,
                                                        hmd_end_datetime   = self.end_date,
                                                        hmd_freq_type      = self.freq_type)
    
        for (start_datetime, bid_price, ask_price) in hist_mkt_prices:
    
            if bid_price == None:
                bid_price = 0
            if ask_price == None:
                ask_price = 0
            # extra code for missing prices
            
            if (bid_price > 0 and ask_price > 0):
                midpoint_price = bid_price + ((ask_price - bid_price)/2)
            else:
                if bid_price > 0:
                    midpoint_price = bid_price
                elif ask_price > 0:
                    midpoint_price = ask_price
                else:
                    midpoint_price = 0
                    
            bid_price_list.append(bid_price)
            ask_price_list.append(ask_price)
            midpoint_price_list.append(midpoint_price)
            date_list.append(start_datetime)
    
            # record price record in the prices dataframe which contains all records
    
            index_val = self.inv_ticker 
            
            format_date = start_datetime.strftime('%m/%d %H:%M')
            temp_df = pd.DataFrame({format_date: (float(midpoint_price))}, index=[index_val] )
    
            # added sort=False for future behavior
            #prices_df = pd.concat([prices_df, temp_df])
            
            prices_df = pd.concat([prices_df, temp_df], sort=False)
    
        prices_df = prices_df.groupby(prices_df.index).sum()
    
        if len(prices_df.index) > 0:
            prices_df = fixit_df(prices_df)
    
        return bid_price_list, ask_price_list, midpoint_price_list, date_list, prices_df


    def graph_results(self, prices_df, streaks_df, lookback_threshold):
    
        # graph the prices dataframe then save the figure
    
        plt.style.use(VISUAL_STYLE_TO_USE)
    
        fig = plt.figure(figsize=(20,10))
        ax1 = fig.add_subplot(111)
    
        plt.xlabel('Date & Time', color=VISUAL_XAXIS_COLOR) # this sets the axes title colour only
        plt.ylabel('Price', color=VISUAL_XAXIS_COLOR)       # this sets the axes title colour only
    
        # change title to use dollar threshold rather than streak percent
        
        #maintitle = "%s Streaks between %s and %s using %3.2f dollar threshold looking back %1.0f records with profit $%5.2f" % \
        #    (self.inv_ticker, self.start_date, self.end_date, STREAK_LOOKBACK_THRESHOLD, STREAK_LOOKBACK_PERIOD, self.profit)
        maintitle = "%s Streaks between %s and %s using %3.3f dollar threshold looking back %1.0f records with profit $%5.2f" % \
            (self.inv_ticker, self.start_date, self.end_date, lookback_threshold, STREAK_LOOKBACK_PERIOD, self.profit)
        plt.suptitle(maintitle, fontsize=VISUAL_TOP_TITLE_FONT_SIZE)
        
        lowertitle = "Number Ups: %2.0f  Number Downs: %2.0f  Number Flats: %2.0f" % (self.number_ups, self.number_downs, self.number_flats)
        plt.title(lowertitle, fontsize=VISUAL_2ND_TITLE_FONT_SIZE)
    
        prices_df.plot(ax=ax1, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color = VISUAL_GREY_LINE_COLOR)
        
        total_streaks = len(streaks_df.index)
        
        if total_streaks > 0:
            new_streaks_df = pd.DataFrame(index=prices_df.index)
    
            # next line then puts the streak data frame this TransactionDB onto the same dataframe
            # added sort=False for future behavior
            #graph_streaks_df1 = pd.concat([new_streaks_df, streaks_df], axis = 1)
            
            graph_streaks_df1 = pd.concat([new_streaks_df, streaks_df], axis = 1, sort=False)
    
            # next line interpolate - forward which fills in the gap after the first record
            # pro-rating values after it til it reaches the end
            # sadly it continues to use this value right until the end which the second interpolate statement fixs
            
            graph_streaks_df2 = graph_streaks_df1.interpolate(method = 'linear', limit_direction='forward')
            graph_streaks_df3 = graph_streaks_df2.interpolate(axis=0).where(graph_streaks_df1.bfill(axis=0).notnull())
            graph_streaks_df3.plot(ax=ax1, kind=VISUAL_GRAPH_KIND, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT*2, color=VISUAL_COLOR_LIST[2])  # red
    
        # way do plotting depends on how many plots handling at once - one or many
        
        box = ax1.get_position()
        ax1.set_position([box.x0, box.y0, box.width * VISUAL_LEGEND_BBOX_WIDTH, box.height])
        ax1.legend(loc='center left', bbox_to_anchor=VISUAL_LEGEND_BBOX_TO_ANCHOR, ncol=VISUAL_LEGEND_COLS, fancybox=VISUAL_LEGEND_FANCYBOX, fontsize=VISUAL_LEGEND_FONT_SIZE)
    
        # use every 20th records for the xticks
        
        ticks_to_use = prices_df.index[::5]
    
        ax1.set_xticklabels(ticks_to_use, rotation=VISUAL_XAXIS_ROTATION)
        plt.grid(linestyle="dotted", color='grey', linewidth=0.5)
    
        # now save the graphs to the specified pathname
        
        disp_start_date = self.start_date[0:10]
        disp_end_date = self.end_date[0:10]
        
        # change filename to include dollar threshold not percentage
        
        #output_file_name = "%sSTREAKS_%s_%s_%s_%2.2f_%1.0f_$%5.2f.png" % \
        #    ( self.output_path_name, self.inv_ticker, disp_start_date, disp_end_date, STREAK_LOOKBACK_THRESHOLD, STREAK_LOOKBACK_PERIOD, self.profit )
        output_file_name = "%sSTREAKS_%s_%s_%s_%2.3f_%1.0f_$%5.2f.png" % \
            ( self.output_path_name, self.inv_ticker, disp_start_date, disp_end_date, lookback_threshold, STREAK_LOOKBACK_PERIOD, self.profit )
        
        plt.savefig(output_file_name)
    
        # Only actually display if boolean says so
    
        if self.display_to_screen:
            plt.show()
    
        return


    def record_streak(self, start_index, end_index, start_bid_price, start_ask_price, end_bid_price, end_ask_price, start_midpoint_price, end_midpoint_price, start_datetime, end_datetime, direction, streaks_df):
    
        # work out the date less the time for the summary record
        
        this_date    = start_datetime.date()
        local_profit = 0
           
        if direction == 'up':
            self.number_ups += 1
            local_profit += (end_ask_price - start_bid_price) * 100
    
        if direction == 'down':
            self.number_downs += 1
            local_profit += (end_bid_price - start_ask_price) * 100
    
        if direction == 'flat':
            self.number_flats += 1
            local_profit += 0
    
        self.profit += local_profit
    
        # work out if this streak summary record already exists and if so get the sts_id
        
        this_inv_ticker = self.inv_ticker 
        
        sts_ids = get_streak_summary_id(database       = self.database,
                                        sts_inv_ticker = this_inv_ticker, 
                                        start_date     = this_date, 
                                        end_date       = this_date, 
                                        sts_threshold  = STREAK_LOOKBACK_THRESHOLD)
        if sts_ids == []:
            
            this_sts_id = ''
            #print('creating new streak summary record')
            
            
            this_streak_summaries = ImsStreakSummarieDB(database       = self.database,
                                                        sts_id         = this_sts_id,
                                                        sts_inv_ticker = this_inv_ticker,
                                                        sts_date       = this_date,
                                                        sts_threshold  = STREAK_LOOKBACK_THRESHOLD)
        
            this_streak_summaries.insert_DB()
            
            # now get it again
            
            sts_ids = get_streak_summary_id(database       = self.database,
                                            sts_inv_ticker = this_inv_ticker, 
                                            start_date     = this_date, 
                                            end_date       = this_date, 
                                            sts_threshold  = STREAK_LOOKBACK_THRESHOLD)[0]
            
            for (this_sts_id) in sts_ids:
                if DEBUG:
                    print('this_sts_id', this_sts_id)
        
        else:
            
            #print('does exist so pick it off')
            for (this_sts_id) in sts_ids[0]:
                if DEBUG:
                    print('this_sts_id', this_sts_id)
                
       
            
        if DEBUG:
            print('****** recording this is a streak *****')
            print('direction ', direction )
            print('start_index ', start_index, ' end_index ', end_index )
            print('start_bid_price ', start_bid_price, ' start_ask_price ', start_ask_price )
            print('end_bid_price ', end_bid_price, ' end_ask_price ', end_ask_price )
            print('start_datetime ', start_datetime, ' end_datetime ', end_datetime )
            print('profit = ', self.profit)
            print('***************************************')
    
        # write a record to streak_details here
    
        this_streak_details = ImsStreakDetailDB(database                  = self.database,
                                                std_sts_id                = this_sts_id,
                                                std_streak_start_datetime = start_datetime,
                                                std_streak_end_datetime   = end_datetime,
                                                std_streak_type           = direction,
                                                std_start_bid_price       = start_bid_price,
                                                std_start_ask_price       = start_ask_price,
                                                std_end_bid_price         = end_bid_price,
                                                std_end_ask_price         = end_ask_price,
                                                std_start_spread          = (start_ask_price - start_bid_price),
                                                std_end_spread            = (end_ask_price - end_bid_price),
                                                std_profit                = self.profit)
        
        this_streak_details.insert_DB()
    
        index_val   = self.inv_ticker 
        format_date = start_datetime.strftime('%m/%d %H:%M')
    
        if len(streaks_df.index) == 0:
            format_date = start_datetime.strftime('%m/%d %H:%M')
            temp_df     = pd.DataFrame({format_date: (float(start_midpoint_price))}, index=[index_val] )
            streaks_df  = pd.concat([streaks_df, temp_df], sort=False)
    
        format_date = end_datetime.strftime('%m/%d %H:%M')
        temp_df     = pd.DataFrame({format_date: (float(end_midpoint_price))}, index=[index_val] )
        streaks_df  = pd.concat([streaks_df, temp_df], sort=False)
    
        return streaks_df


    def streak_has_ended(self, price_list, current_index, current_direction, lookback_period, lookback_threshold):
    
        ended = False
    
        if current_index >= lookback_period:
    
            # we will not go before start of datastructure
            
            lookback_value = price_list[current_index - lookback_period]
            current_value  = price_list[current_index]
    
            if current_direction == 'up':
                if current_value < lookback_value:
                    if (lookback_value - current_value) > lookback_threshold:
                        ended = True
    
            if current_direction == 'down':
                if current_value > lookback_value:
                    if (current_value - lookback_value) > lookback_threshold:
                        ended = True
    
            if current_direction == 'flat':
                if abs(lookback_value - current_value) >= lookback_threshold:
                    ended = True
    
        if DEBUG:
    
            if current_index >= lookback_period:
                if ended:
                    print('In streak_has_ended, Lookback_value = ',lookback_value,' Current_value = ',current_value)
    
        return ended
