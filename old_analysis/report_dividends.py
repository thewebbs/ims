#------------------------------------------------------------
# Filename  : report_dividends.py
# Project   : ava
#
# Descr     : This file contains code to produce the daily 
#             dividend reports. Based on report_daily but
#             broken down into constituent parts.
#
# Params    : ticker (or %)
#             display_report Y/N
#             filename excluding path
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2022-01-01   1 MW  Initial write
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from datetime import datetime
from database.db_objects.ImsDividendDB import get_daily_div_rows, get_dividends_in_next_period
from database.db_objects.ImsHistMktDataDB import get_latest_start_datetime_before_date_for_ticker, get_midpoint_for_date
from utils.config import REPORT_FOLDER_OSX
from utils.utils_database import open_db, close_db
from utils.utils_HTML import create_doc, finish_doc
from utils.utils_sys import get_sys_params
import webbrowser

def report_dividends(database, inv_ticker, str_run_date, div_filename, upcoming_div_filename, display_report, upcoming_period, days_urgent):
    
    print("Started report_daily")
    print(" ")
    print("Producing daily dividend report")   
    
    produce_daily_div_report(database       = database, 
                              inv_ticker     = inv_ticker, 
                              str_run_date   = str_run_date, 
                              div_filename   = div_filename, 
                              display_report = display_report)
    
    print("Finished daily dividends report") 

    print("Producing upcoming dividends report")   
    
    produce_upcoming_div_report(database               = database, 
                                 inv_ticker            = inv_ticker, 
                                 str_run_date          = str_run_date, 
                                 upcoming_div_filename = upcoming_div_filename, 
                                 display_report        = display_report, 
                                 upcoming_period       = upcoming_period, 
                                 days_urgent           = days_urgent)
    
    print("Finished upcoming dividends report") 
    
    return
    
    

def produce_daily_div_report(database, inv_ticker, str_run_date, div_filename, display_report):
    
    div_rows = get_daily_div_rows(database   = database, 
                                  inv_ticker = inv_ticker)
    
    new_div_rows = []
    
    for div_rec in div_rows:
        this_ticker            = div_rec[0]
        this_as_of_date        = div_rec[1]
        this_div_per_share     = div_rec[2]
        this_sec_name          = div_rec[3]
        
        str_this_as_of_date = this_as_of_date.strftime("%Y-%m-%d") + ' 13:59:59'
        # get the last start_datetime before this_as_of_date
        last_datetime = get_latest_start_datetime_before_date_for_ticker(database           = database,
                                                                         hmd_inv_ticker     = this_ticker, 
                                                                         hmd_start_datetime = str_this_as_of_date)
        
        if last_datetime != None:
            # get the midpoint price closest to this_as_of_date
            midpoint_price = get_midpoint_for_date(database           = database,
                                                   hmd_inv_ticker     = this_ticker, 
                                                   hmd_start_datetime = last_datetime, 
                                                   hmd_freq_type      = '1 min')[0]
        
            if midpoint_price != None:
                
                percent_div = 100 * (this_div_per_share / midpoint_price)
            
            
                new_div_rec = (this_ticker, this_as_of_date, this_div_per_share, this_sec_name, midpoint_price, percent_div)
                new_div_rows.append(new_div_rec)
    
    sorted_div_rows = sorted(new_div_rows, key=lambda tup: tup[5], reverse=True)
    
    latest_daily_div_report(div_rows        = sorted_div_rows, 
                            inv_ticker      = inv_ticker, 
                            str_run_date    = str_run_date, 
                            div_filename    = div_filename, 
                            display_report  = display_report)
    
    
    return 


def produce_upcoming_div_report(database, inv_ticker, str_run_date, upcoming_div_filename, display_report, upcoming_period, days_urgent):
    
    upcoming_div_rows = get_dividends_in_next_period(database        = database, 
                                                     inv_ticker      = inv_ticker, 
                                                     upcoming_period = upcoming_period, 
                                                     days_urgent     = days_urgent)
    
    latest_upcoming_div_report(upcoming_div_rows = upcoming_div_rows, 
                               inv_ticker        = inv_ticker, 
                               str_run_date      = str_run_date, 
                               upcoming_div_filename = upcoming_div_filename, 
                               display_report    = display_report)
    
    return 


def latest_daily_div_report(div_rows, inv_ticker, str_run_date, div_filename, display_report):
    
    if inv_ticker == '%':
        html = "<h1> Daily Dividend Report for All Tickers on %s </h1>" % (str_run_date)
    else:
        html = "<h1> Daily Dividend Report for %s for %s </h1>" % (inv_ticker, str_run_date) 
        
    f = open(div_filename,'w')
    font_size = 10
    create_doc(f, font_size)

    first_time = True
    
    for div_rec in div_rows:
        this_ticker            = div_rec[0]
        this_as_of_date        = div_rec[1]
        this_div_per_share     = div_rec[2]
        this_sec_name          = div_rec[3]
        this_midpoint_price    = div_rec[4]
        this_percent_div       = div_rec[5]
        
        if this_div_per_share == None:
            this_div_per_share = 0
        if this_midpoint_price == None:
            this_midpoint_price = 0
        if this_percent_div == None:
            this_percent_div = 0
        
        if first_time:
            
            first_time = False       
            html += "</table><br>"    
            html += "<table class=\"font10\"> <tr class=\"head\"> "
            html += "<td class=\"descr\"> Ticker </td>"
            html += "<td class=\"descr\"> Sector </td>"
            html += "<td class=\"descr\"> Date </td>"
            html += "<td class=\"smallvalue\"> Div Per Share </td> " 
            html += "<td class=\"smallvalue\"> Midpoint </td> " 
            html += "<td class=\"smallvalue\"> % Div </td> "
            html += "</tr>" 
            
        html += "<tr> <td class=\"descr\"> %s </td>"     % (this_ticker)
        html += "<td class=\"descr\"> %s </td>"          % (this_sec_name)
        html += "<td class=\"descr\"> %s </td>"          % (this_as_of_date)
        html += "<td class=\"smallvalue\"> %.2f </td>"   % (this_div_per_share) 
        html += "<td class=\"smallvalue\"> %.2f </td>"   % (this_midpoint_price) 
        html += "<td class=\"smallvalue\"> %.2f </td> "  % (this_percent_div)
        html += "</tr>" 
                        
    html += "</table><br>"

    f.write(html)
    finish_doc(f)
    f.close()
    
    if display_report == 'Y':
        html_link = 'file://%s' % (div_filename)
        webbrowser.open(html_link)

    return


def latest_upcoming_div_report(upcoming_div_rows, inv_ticker, str_run_date, upcoming_div_filename, display_report):
    
    if inv_ticker == '%':
        html = "<h1> Daily Upcoming Div Report for All Tickers on %s </h1>" % (str_run_date)
    else:
        html = "<h1> Daily Upcoming Div Report for %s for %s </h1>" % (inv_ticker, str_run_date) 
        
    f = open(upcoming_div_filename,'w')
    font_size = 10
    create_doc(f, font_size)

    first_time = True

    html += "<h2> Upcoming Dividend Dates <h2>"
    for upcoming_div_rec in upcoming_div_rows:
        this_ticker                    = upcoming_div_rec[0]
        this_as_of_date                = upcoming_div_rec[1]
        this_div_period                = upcoming_div_rec[2]
        this_div_per_share             = upcoming_div_rec[3]
        this_next_as_of_date           = upcoming_div_rec[4]
        this_urgency                   = upcoming_div_rec[5]
            
        if first_time:
            
            first_time = False       
            html += "</table><br>"    
            html += "<table class=\"font10\"> <tr class=\"head\"> "
            html += "<td class=\"descr\"> Ticker </td>"
            html += "<td class=\"descr\"> Last As Of Date </td>"
            html += "<td class=\"descr\"> Dividend Period </td>"
            html += "<td class=\"descr\"> Dividends Per Share </td> " 
            html += "<td class=\"descr\"> Next As Of Date </td>"
            html += "</tr>"
            
        html += "<tr> <td class=\"descr\"> %s </td>"          % (this_ticker)
        html += "<td class=\"descr\"> %s </td>"               % (this_as_of_date)
        html += "<td class=\"descr\"> %s </td>"               % (this_div_period)
        html += "<td class=\"descr\"> %.2f </td>"             % (this_div_per_share) 
        if this_urgency == 'Urgent':
            html += "<td class=\"lossfont\"> %s </td>"        % (this_next_as_of_date)
        else:
            html += "<td class=\"descr\"> %s </td>"           % (this_next_as_of_date)
            
                    
    html += "</table><br>"
    
    f.write(html)
    finish_doc(f)
    f.close()
    
    if display_report == 'Y':
        html_link = 'file://%s' % (upcoming_div_filename)
        webbrowser.open(html_link)

    return


if __name__ == "__main__":
    
    (num_args, args_list) = get_sys_params()
    
    if num_args == 0:
        inv_ticker            = '%'
        display_report        = 'Y'
        div_filename          = REPORT_FOLDER_OSX + 'report_daily_div.html'
        upcoming_div_filename = REPORT_FOLDER_OSX + 'report_upcoming_divs.html'
        upcoming_period_number = "'7'"
        upcoming_period_duration = 'DAY'
        upcoming_period = upcoming_period_number + ' ' + upcoming_period_duration
        days_urgent           = 3
        
        
    else:
        if num_args == 7:
            
            removed_brackets = args_list.strip("[]")
            split_args_list = removed_brackets.split(",")
            
            inv_ticker            = split_args_list[0].strip("'")
            display_report        = split_args_list[1].strip(" ").strip("'")
            div_filename          = split_args_list[5].strip(" ").strip("'")   
            upcoming_div_filename = split_args_list[6].strip(" ").strip("'")
            days_urgent           = int(split_args_list[7].strip(" ").strip("'"))
            
            # Note - yes I really do mean to be stripping off double but not single quotes here. Please leave alone
            upcoming_period_number  = split_args_list[8].strip(" ").strip('"') 
            upcoming_period_duration = split_args_list[9].strip(" ").strip("'")
            upcoming_period = upcoming_period_number + ' ' + upcoming_period_duration
            
            print(' ')          
            print('parameters received', inv_ticker, display_report, div_filename, upcoming_div_filename, days_urgent, upcoming_period)
            print(' ')
            
    if (num_args != 7 and num_args != 0):
        print("ERROR report_dividends.py - wrong number of args provided expected 7")
        print("Number args: ", num_args)
        print("Arg list: ", args_list)
        
    else:
        
        print("report_dividends - Open Database")
            
        database = open_db(host        = DB_HOST, 
                           port        = DB_PORT, 
                           tns_service = DB_TNS_SERVICE, 
                           user_name   = DB_USER_NAME, 
                           password    = DB_PASSWORD)  
             
        # Create report for last business day
        
        today_day_of_week = datetime.today().weekday()
        
        if today_day_of_week == 6 or today_day_of_week == 0:
            # if Sunday or Monday go back 3 days
            num_days = 3
        else: 
            # any other day go back  day
            num_days = 1
        
        str_run_date = datetime.strftime(datetime.now(), '%y-%m-%d')
        
        report_dividends(database              = database, 
                         inv_ticker            = inv_ticker, 
                         str_run_date          = str_run_date, 
                         div_filename          = div_filename, 
                         upcoming_div_filename = upcoming_div_filename, 
                         display_report        = display_report, 
                         days_urgent           = days_urgent,
                         upcoming_period       = upcoming_period 
                     )
    
        print("report_dividends - Close Database")
            
        close_db(database = database)   

    
 
