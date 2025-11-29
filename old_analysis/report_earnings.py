#------------------------------------------------------------
# Filename  : report_earnings.py
# Project   : ava
#
# Descr     : This file contains code to produce the daily 
#             earning reports. Based on report_earnings but
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
from database.db_objects.ImsEarningDB import get_daily_ear_rows, get_earnings_in_next_period
from database.db_objects.ImsHistMktDataDB import get_latest_start_datetime_before_date_for_ticker, get_midpoint_for_date
from utils.config import REPORT_FOLDER_OSX
from utils.utils_database import open_db, close_db
from utils.utils_HTML import create_doc, finish_doc
from utils.utils_sys import get_sys_params
import webbrowser

def report_earnings(database, inv_ticker, str_run_date, earn_filename, upcoming_earn_filename, display_report, upcoming_period, days_urgent):
    
    print("Started report_earnings")
    print(" ")
    print("Producing daily earning report")   
    
    produce_daily_earn_report(database       = database, 
                              inv_ticker     = inv_ticker, 
                              str_run_date   = str_run_date, 
                              earn_filename   = earn_filename, 
                              display_report = display_report)
    
    print("Finished daily earnings report") 

    print("Producing upcoming earnings report")    
    
    produce_upcoming_earn_report(database               = database, 
                                  inv_ticker             = inv_ticker, 
                                  str_run_date           = str_run_date, 
                                  upcoming_earn_filename = upcoming_earn_filename, 
                                  display_report         = display_report, 
                                  upcoming_period        = upcoming_period, 
                                  days_urgent            = days_urgent)
    
    print("Finished upcoming earnings report") 
    
    return
    
    
def produce_daily_earn_report(database, inv_ticker, str_run_date, earn_filename, display_report):
    
    earn_rows = get_daily_ear_rows(database   = database, 
                                  inv_ticker = inv_ticker)
    
    new_earn_rows = []
    
    for earn_rec in earn_rows:
        this_ticker            = earn_rec[0]
        this_as_of_date        = earn_rec[1]
        this_earn_per_share     = earn_rec[2]
                
        str_this_as_of_date = this_as_of_date.strftime("%Y-%m-%d") + ' 13:59:59'
  
    #sorted_earn_rows = sorted(new_earn_rows, key=lambda tup: tup[5], reverse=True)
    
    latest_daily_earn_report(earn_rows       = earn_rows, 
                             inv_ticker      = inv_ticker, 
                             str_run_date    = str_run_date, 
                             earn_filename   = earn_filename, 
                             display_report  = display_report)
    
    
    return 


def produce_upcoming_earn_report(database, inv_ticker, str_run_date, upcoming_earn_filename, display_report, upcoming_period, days_urgent):
    
    upcoming_earn_rows = get_earnings_in_next_period(database        = database, 
                                                    inv_ticker      = inv_ticker, 
                                                    upcoming_period = upcoming_period, 
                                                    days_urgent     = days_urgent)
    
    latest_upcoming_earn_report(upcoming_earn_rows      = upcoming_earn_rows, 
                                inv_ticker             = inv_ticker, 
                                str_run_date           = str_run_date, 
                                upcoming_earn_filename = upcoming_earn_filename, 
                                display_report         = display_report)
    
    return 


def latest_daily_earn_report(earn_rows, inv_ticker, str_run_date, earn_filename, display_report):
    
    if inv_ticker == '%':
        html = "<h1> Daily Earning Report for All Tickers on %s </h1>" % (str_run_date)
    else:
        html = "<h1> Daily Earning Report for %s for %s </h1>" % (inv_ticker, str_run_date) 
        
    f = open(earn_filename,'w')
    font_size = 10
    create_doc(f, font_size)

    first_time = True
    
    for earn_rec in earn_rows:
        this_ticker            = earn_rec[0]
        this_as_of_date        = earn_rec[1]
        this_earn_per_share     = earn_rec[2]
        #this_sec_name          = earn_rec[3]
        
        if this_earn_per_share == None:
            this_earn_per_share = 0
        
        if first_time:
            
            first_time = False       
            html += "</table><br>"    
            html += "<table class=\"font10\"> <tr class=\"head\"> "
            html += "<td class=\"descr\"> Ticker </td>"
            #html += "<td class=\"descr\"> Sector </td>"
            html += "<td class=\"descr\"> Date </td>"
            html += "<td class=\"smallvalue\"> Earn Per Share </td> " 
            html += "</tr>" 
            
        html += "<tr> <td class=\"descr\"> %s </td>"     % (this_ticker)
        #html += "<td class=\"descr\"> %s </td>"          % (this_sec_name)
        html += "<td class=\"descr\"> %s </td>"          % (this_as_of_date)
        html += "<td class=\"smallvalue\"> %.2f </td>"   % (this_earn_per_share) 
        html += "</tr>" 
                        
    html += "</table><br>"

    f.write(html)
    finish_doc(f)
    f.close()
    
    if display_report == 'Y':
        html_link = 'file://%s' % (earn_filename)
        webbrowser.open(html_link)

    return


def latest_upcoming_earn_report(upcoming_earn_rows, inv_ticker, str_run_date, upcoming_earn_filename, display_report):
    
    if inv_ticker == '%':
        html = "<h1> Daily Upcoming Earnings Report for All Tickers on %s </h1>" % (str_run_date)
    else:
        html = "<h1> Daily Upcoming Earnings Report for %s for %s </h1>" % (inv_ticker, str_run_date) 
        
    f = open(upcoming_earn_filename,'w')
    font_size = 10
    create_doc(f, font_size)

    first_time = True
    html += "<h2> Upcoming Earnings Dates <h2>"
    
    for upcoming_ear_rec in upcoming_earn_rows:
        this_ticker                    = upcoming_ear_rec[0]
        this_as_of_date                = upcoming_ear_rec[1]
        this_ear_period                = upcoming_ear_rec[2]
        this_ear_per_share             = upcoming_ear_rec[3]
        this_next_as_of_date           = upcoming_ear_rec[4]
        this_urgency                   = upcoming_ear_rec[5]
            
        if first_time:
            
            first_time = False       
            html += "</table><br>"    
            html += "<table class=\"font10\"> <tr class=\"head\"> "
            html += "<td class=\"descr\"> Ticker </td>"
            html += "<td class=\"descr\"> Last As Of Date </td>"
            html += "<td class=\"descr\"> Earnings Period </td>"
            html += "<td class=\"descr\"> Earnings Per Share </td> " 
            html += "<td class=\"descr\"> Next As Of Date </td>"
            html += "</tr>"
            
        html += "<tr> <td class=\"descr\"> %s </td>"          % (this_ticker)
        html += "<td class=\"descr\"> %s </td>"               % (this_as_of_date)
        html += "<td class=\"descr\"> %s </td>"               % (this_ear_period)
        html += "<td class=\"descr\"> %.2f </td>"             % (this_ear_per_share) 
        if this_urgency == 'Urgent':
            html += "<td class=\"lossfont\"> %s </td>"        % (this_next_as_of_date)
        else:
            html += "<td class=\"descr\"> %s </td>"           % (this_next_as_of_date)
                    
    html += "</table><br>"
    
    f.write(html)
    finish_doc(f)
    f.close()
    
    if display_report == 'Y':
        html_link = 'file://%s' % (upcoming_earn_filename)
        webbrowser.open(html_link)

    return


if __name__ == "__main__":
    
    (num_args, args_list) = get_sys_params()
    
    if num_args == 0:
        inv_ticker            = '%'
        display_report        = 'Y'
        earn_filename          = REPORT_FOLDER_OSX + 'report_earnings.html'
        upcoming_earn_filename = REPORT_FOLDER_OSX + 'report_upcoming_earnings.html'
        upcoming_period_number = "'7'"
        upcoming_period_duration = 'DAY'
        upcoming_period = upcoming_period_number + ' ' + upcoming_period_duration
        days_urgent           = 3
        
        
    else:
        if num_args == 7:
            
            removed_brackets = args_list.strip("[]")
            split_args_list = removed_brackets.split(",")
            
            inv_ticker             = split_args_list[0].strip("'")
            display_report         = split_args_list[1].strip(" ").strip("'")
            earn_filename          = split_args_list[5].strip(" ").strip("'")   
            upcoming_earn_filename = split_args_list[6].strip(" ").strip("'")
            days_urgent            = int(split_args_list[7].strip(" ").strip("'"))
            
            # Note - yes I really do mean to be stripping off double but not single quotes here. Please leave alone
            upcoming_period_number  = split_args_list[8].strip(" ").strip('"') 
            upcoming_period_duration = split_args_list[9].strip(" ").strip("'")
            upcoming_period = upcoming_period_number + ' ' + upcoming_period_duration
            
            print(' ')          
            print('parameters received', inv_ticker, display_report, earn_filename, upcoming_earn_filename, days_urgent, upcoming_period)
            print(' ')
            
    if (num_args != 8 and num_args != 0):
        print("ERROR report_earnings.py - wrong number of args provided expected 7")
        print("Number args: ", num_args)
        print("Arg list: ", args_list)
        
    else:
        
        print("report_earnings - Open Database")
            
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
        
        report_earnings(database               = database, 
                        inv_ticker             = inv_ticker, 
                        str_run_date           = str_run_date, 
                        earn_filename          = earn_filename, 
                        upcoming_earn_filename = upcoming_earn_filename, 
                        display_report         = display_report, 
                        days_urgent            = days_urgent,
                        upcoming_period        = upcoming_period 
                        )
    
        print("report_earnings - Close Database")
            
        close_db(database = database)   

    
 
