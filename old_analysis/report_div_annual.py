#------------------------------------------------------------
# Filename  : report_div_annual.py
# Project   : ava
#
# Descr     : This file contains code to produce the annual dividend report
#
# Params    : database 
#             year 
#             include_current_month  
#             include_markets
#             update_exd
#             use_inc_totals
#             show_details
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2014-01-01   1 DW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from utils.config import DEBUG, PERCENT_DOWN_THRESHOLD, PERCENT_UP_THRESHOLD
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import open_db, close_db 
from datetime import date
from decimal import Decimal
from database.db_objects.DividendsPaidDB import get_dividends_paid_tot
from database.db_objects.ExpectedDividendDB import get_divs_expected_year, setup_expected_divs
from database.db_objects.InterestAccruedDB import get_interest_accrued
from database.db_objects.PositionDB import get_investments, get_positions_by_month, get_values_by_month
from database.db_objects.InvestmentDB import get_inv_name
from database.db_objects.PortfolioDB import get_portfolios
from utils.utils_HTML import create_doc, finish_doc
from utils.utils_file import get_report_folder
import webbrowser

 
def display_actual_divs(actual_divs, expected_divs, year):
    
    current_month = date.today().month
    current_year  = date.today().year

    html_actual = "<tr style=\"background: #eee;\"> <td class=\"border\"> Actual Dividends</td> "
 
    tot_actual_divs = Decimal("0.0")

    for actual_div_ind in range(12):
        the_actual_div    = actual_divs[actual_div_ind]
        the_expected_div = expected_divs[actual_div_ind]
        
        if the_actual_div == None:
            the_actual_div = Decimal("0.0")
    
        if the_expected_div == None:
            the_expected_div = Decimal("0.0")
    
        if the_expected_div > 0:
            percent_change = ((the_actual_div - the_expected_div)/the_expected_div) * 100
        else:
            percent_change = 0  
                        
        if (percent_change > PERCENT_UP_THRESHOLD):
            html_actual += "<td class=\"gainsmallvalue\"> %10.0f </td>" % (the_actual_div)
        
        elif (percent_change < PERCENT_DOWN_THRESHOLD):
            html_actual += "<td class=\"losssmallvalue\"> %10.0f </td>" % (the_actual_div)
            
        else:       
            html_actual += "<td class=\"smallvalue\"> %10.0f </td>" % (the_actual_div)
        
        tot_actual_divs += the_actual_div
        
    html_actual += "<td class=\"smallvalue\"> %10.0f </td> </tr>" % (tot_actual_divs) 
     
    return html_actual

 
def display_combined_divs(year, include_current_month,  include_markets, use_inc_totals):
    
    # for the combined portfolio create html to display the expected dividends, actual dividends and variance
    
    html_combined = display_totals("%", year, include_current_month, include_markets, use_inc_totals)
    
    return html_combined


def display_details(database, por_name, year, include_current_month, include_markets, use_inc_totals):
 
    # for the portfolio display each stock and its actual/expected/variance
    
    start_of_year = date(year, 1, 1).strftime("%Y-%m-%d")
    end_of_year   = date(year, 12, 31).strftime("%Y-%m-%d")

    investments   = get_investments(database = database, 
                                    por_name = por_name, "%", "%", "%", start_of_year, end_of_year, use_inc_totals)
    
    
    html_details = ""
    
    for (inv_ticker, inv_exc_symbol) in investments:
        
        the_name = get_inv_name(database       = database,
                                inv_ticker     = inv_ticker, 
                                inv_exc_symbol = inv_exc_symbol)
        
        
        html_details += "<table class=\"font10\"> <tr class=\"head\"> <td class=\"descrlong\">" + inv_ticker + " - " + the_name + \
                "<td class=\"smallvalue\"> JAN </td> <td class=\"smallvalue\"> FEB </td> <td class=\"smallvalue\"> MAR </td> <td class=\"smallvalue\"> APR </td>" + \
                "<td class=\"smallvalue\"> MAY </td> <td class=\"smallvalue\"> JUN </td> <td class=\"smallvalue\"> JUL </td> <td class=\"smallvalue\"> AUG </td>" + \
                "<td class=\"smallvalue\"> SEP </td> <td class=\"smallvalue\"> OCT </td> <td class=\"smallvalue\"> NOV </td> <td class=\"smallvalue\"> DEC </td>" + \
                "<td class=\"smallvalue\"> TOTAL </td> </tr>" 
    
        # Display the actual divs
        
        market_values = get_values_by_month(database       = database,
                                            por_name       = por_name, 
                                            inv_ticker     = inv_ticker, 
                                            inv_exc_symbol = inv_exc_symbol, 
                                            inv_ccy_type   = "CAD", 
                                            year           = year, 
                                            use_inc_totals = use_inc_totals)
        
        # actual position units
        
        position_values = get_positions_by_month(database       = database,
                                                 por_name       = por_name, 
                                                 inv_ticker     = inv_ticker, 
                                                 inv_exc_symbol = inv_exc_symbol, 
                                                 inv_ccy_type   = "CAD", 
                                                 year           = year, 
                                                 use_inc_totals = use_inc_totals)
        
        # Display the expected divs
        
        expected_divs = get_divs_expected_year(database       = database,
                                               por_name       = por_name, 
                                               inv_ticker     = inv_ticker, 
                                               inv_exc_symbol = inv_exc_symbol, 
                                               inv_ccy_type   = "CAD", 
                                               year           = year, 
                                               use_inc_totals = use_inc_totals)
  
        (html_expected) = display_expected_divs(expected_divs = expected_divs,  
                                                market_values = market_values, 
                                                show_details  = show_details)
        html_details += html_expected
        
        # Display the dividends paid
  
        actual_divs = get_dividends_paid_tot(database       = database,
                                             por_name       = por_name, 
                                             inv_ticker     = inv_ticker, 
                                             inv_exc_symbol = inv_exc_symbol,  
                                             year           = year)
    
        (html_actuals) = display_actual_divs(actual_divs   = actual_divs, 
                                             expected_divs = expected_divs, 
                                             year          = year)
        
        html_details += html_actuals
 
        # Display the market values
 
        if include_markets:
     
            markets = get_values_by_month(database       = database,
                                          por_name       = por_name, 
                                          inv_ticker     = inv_ticker, 
                                          inv_exc_symbol = inv_exc_symbol, 
                                          inv_ccy_type   = "CAD", 
                                          year           = year, 
                                          use_inc_totals = use_inc_totals)
            
            grey_background = False
            html_details    += display_markets(grey_background, markets)
                 
        grey_background = True
        html_details    += display_positions(grey_background, position_values)
       
        html_details += "</table><br>"
            
    return html_details
   

def display_expected_divs(expected_divs, market_values, show_details):
      
    html_expected     = "<tr style=\"background: #eee;\"> <td class=\"border\"> Expected Dividends</td> "
    tot_expected_divs = Decimal("0.0")
    tot_percent       = Decimal("0.0")
    
    for the_div in expected_divs:
        
        html_expected += "<td class=\"smallvalue\"> %10.0f </td>" % (the_div)
    
        tot_expected_divs += the_div
        
    html_expected += "<td class=\"smallvalue\"> %10.0f </td> </tr>" % (tot_expected_divs) 
   
    return html_expected


def display_interestaccrued(grey_background, interestaccrued):
      
    current_month = date.today().month
    
    if grey_background:
        html_interestaccrued = "<tr style=\"background: #eee;\"> <td class=\"border\"> Interest Accrued</td> "
    else:
        html_interestaccrued = "<tr> <td class=\"border\">  Interest </td> "
    
    tot_interestaccrued = Decimal("0.0")
    has_been_a_value    = False
    lastvalue           = Decimal("0.0")
    
    for month_ind in range(12):
        the_interestaccrued = interestaccrued[month_ind]
                      
        if the_interestaccrued == None:
            the_interestaccrued =  Decimal("0.0")
            
            if has_been_a_value:
                the_interestaccrued = lastvalue
        else:
            has_been_a_value = True
            if month_ind < (current_month-1):
                lastvalue = the_interestaccrued
                
            if month_ind >= (current_month-1):
                the_interestaccrued = lastvalue
            
        html_interestaccrued += "<td class=\"smallvalue\"> %10.0f </td>" % (the_interestaccrued)
        tot_interestaccrued  += the_interestaccrued
        
    html_interestaccrued += "<td class=\"smallvalue\"> %10.0f </td> </tr>" % (tot_interestaccrued) 
    
    return html_interestaccrued


def display_netincome(grey_background, expected_divs, actual_divs, interestaccrued):
    
    current_month = date.today().month
        
    if grey_background:
        html_netincome = "<tr style=\"background: #eee;\"> <td class=\"border\"> Net Income </td> "
    else:
        html_netincome = "<tr> <td class=\"border\">  Net Income </td> "
    
    tot_netincome    = Decimal("0.0")
    has_been_a_value = False
    lastvalue        = Decimal("0.0")
       
    for month_ind in range(12):
        the_expected_divs = expected_divs[month_ind]
        
        if the_expected_divs == None:
            the_expected_divs = Decimal("0.0")
            
        the_actual_divs = actual_divs[month_ind]
        
        if the_actual_divs == None:
            the_actual_divs = Decimal("0.0")
            
        the_interestaccrued = interestaccrued[month_ind]
                   
        if the_interestaccrued == None:
            the_interestaccrued = Decimal("0.0")
            
            if has_been_a_value:
                the_interestaccrued = lastvalue
        
        else:
            has_been_a_value = True  
            if month_ind < (current_month-1):
                lastvalue = the_interestaccrued
            
            if month_ind >= (current_month-1):
                the_interestaccrued = lastvalue
            
        # if there is an actual div for a month, use that, otherwise use expected div
  
        if the_actual_divs > 0:
            the_netincome = the_actual_divs + the_interestaccrued
        else:     
            the_netincome = the_expected_divs + the_interestaccrued
        
        if the_netincome == None:
            the_netincome = Decimal("0.0")

        html_netincome += "<td class=\"smallvalue\"> %10.0f </td>" % (the_netincome)
        tot_netincome  += the_netincome
        
    html_netincome += "<td class=\"smallvalue\"> %10.0f </td> </tr>" % (tot_netincome)
   
    return html_netincome

    
def display_markets(grey_background, market_values):
      
    if grey_background:
        html_markets = "<tr style=\"background: #eee;\"> <td class=\"border\"> Market Value </td> "
    else:
        html_markets = "<tr> <td class=\"border\"> Market Value </td> "
    
    for market_ind in range(12):
        the_market = market_values[market_ind]
        if the_market == None:
            the_market =  Decimal("0.0")
            
        html_markets += "<td class=\"smallvalue\"> %10.0f </td>" % (the_market)
        
    html_markets += "<td class=\"smallvalue\">  </td> </tr>" 
    
    return html_markets


def display_positions(grey_background, position_values):
      
    if grey_background:
        html_positions = "<tr style=\"background: #eee;\"> <td class=\"border\"> PositionDB Units </td> "
    else:
        html_positions = "<tr> <td class=\"border\"> PositionDB Units </td> "
    
    for position_ind in range(12):
        the_position = position_values[position_ind]
  
        if the_position == None:
            the_position =  Decimal("0.0")
            
        html_positions += "<td class=\"smallvalue\"> %10.0f </td>" % (the_position)
        
    html_positions += "<td class=\"smallvalue\">  </td> </tr>" 
    
    return html_positions

    
def display_portfolio_divs(por_name, year, include_current_month, include_markets, use_inc_totals, show_details):
    
    # for the given portfolio create html to display the expected dividends, actual dividends and variance
    
    html_divs = "<h3> %s </h3>" % (por_name)
    
    html_divs += display_totals(por_name              = por_name, 
                                year                  = year, 
                                include_current_month = include_current_month, 
                                include_markets       = include_markets, 
                                use_inc_totals        = use_inc_totals)
    
    # Now display the individual stocks
    
    html_divs += "<br> " 
    
    if show_details:
        html_divs += display_details(por_name              = por_name, 
                                     year                  = year, 
                                     include_current_month = include_current_month, 
                                     include_markets       = include_markets, 
                                     use_inc_totals        = use_inc_totals)
     
    return html_divs


def display_totals(database, por_name, year, include_current_month, include_markets, use_inc_totals):
    
    start_of_year = date(year, 1, 1).strftime("%Y-%m-%d")
    end_of_year   = date(year, 12, 31).strftime("%Y-%m-%d")
    
    if por_name == "%":
        display_name = "Combined"
    else:
        display_name = por_name
  
    html_totals = "<table class=\"font10\"> <tr class=\"head\"> </td> <td class=\"descrlong\">"  + display_name +  "</td>"\
                  "<td class=\"smallvalue\"> JAN </td> <td class=\"smallvalue\"> FEB </td> <td class=\"smallvalue\"> MAR </td> <td class=\"smallvalue\"> APR </td>" + \
                  "<td class=\"smallvalue\"> MAY </td> <td class=\"smallvalue\"> JUN </td> <td class=\"smallvalue\"> JUL </td> <td class=\"smallvalue\"> AUG </td>" + \
                  "<td class=\"smallvalue\"> SEP </td> <td class=\"smallvalue\"> OCT </td> <td class=\"smallvalue\"> NOV </td> <td class=\"smallvalue\"> DEC </td>" + \
                  "<td class=\"smallvalue\"> TOTAL </td> </tr>"

    # Get the markets

    markets = get_values_by_month(database = database, por_name, "%", "%", "CAD", year, use_inc_totals)
    
    # Get the position units
  
    positions = get_positions_by_month(database = database, por_name, "%", "%", "CAD", year, use_inc_totals)
    
    # Display the expected divs
    
    expected_divs = get_divs_expected_year(database = database, por_name, "%", "%", "CAD", year, use_inc_totals)
               
    (html_expected) = display_expected_divs(expected_divs, markets, show_details)
    html_totals += html_expected
    
    # Display the dividends paid

    actual_divs = get_dividends_paid_tot(database = database, por_name, "%", "%", year)
    
    (html_actuals) = display_actual_divs(actual_divs   = actual_divs, 
                                         expected_divs = expected_divs, 
                                         year          = year)
    html_totals += html_actuals
 
    # display the accrued interest
    
    interestaccrued        = get_interest_accrued(database = database, por_name, "%", year)
    grey_background        = False
    (html_interestaccrued) = display_interestaccrued(grey_background = grey_background, 
                                                     interestaccrued = interestaccrued)
    html_totals            += html_interestaccrued

    # display the net income
    
    grey_background = True
    (html_netincome) = display_netincome(grey_background = grey_background, 
                                         expected_divs   = expected_divs, 
                                         actual_divs     = actual_divs, 
                                         interestaccrued = interestaccrued)
    html_totals += html_netincome
    
    # Display the market values

    if include_markets:
        grey_background = False
        html_totals     += display_markets(grey_background, markets)
        
    html_totals += "</table><br>"  
          
    return html_totals


def display_variance_divs(expected_divs, actual_divs, year, include_current_month):
    
    current_month     = date.today().month
    current_year      = date.today().year
    html_variance     = "<tr> <td class=\"border\"> Variance </td> "
    tot_variance_divs = Decimal("0.0")
    
    if include_current_month:
        month_offset = 0
    else:
        month_offset = 1 
    
    for month in range (len(expected_divs)):
        
        # Check to see the month is before this month because we dont have all the actual info otherwise
        
        if (year < current_year) or (month < current_month - month_offset):
 
            div = actual_divs[month] - expected_divs[month]
            
            if div == -0:
                div = Decimal("0.0")

            if div < 0:
                html_variance += "<td class=\"loss\"> %10.0f </td>" % (div)
            else:
                html_variance += "<td class=\"gain\"> %10.0f </td>" % (div)
 
            tot_variance_divs += div
        else:
            html_variance += "<td class=\"black\"> </td>"

    if tot_variance_divs < 0:
        html_variance += "<td class=\"loss\"> %10.0f </td>" % (tot_variance_divs)
    else:
        html_variance += "<td class=\"gain\"> %10.0f </td>" % (tot_variance_divs)
        
    return html_variance

 
def report_div_annual(database, year, include_current_month, include_markets, update_exd, use_inc_totals, show_details):
    
    # Produce report showing expected and actual dividends for the year
    
    # First make sure the expected dividends for the period are correct
    
    if update_exd:
        start_year_date = date(year, 1, 1)
        end_year_date   = date(year, 12, 31)
        setup_expected_divs(database = database, "%", "%", "%", "%", start_year_date, end_year_date, year)
 
    fname     = get_report_folder() + "div_report_%s.html" % (year) 
    f         = open(fname,'w')
    font_size = 10
    create_doc(f, font_size)
          
    if use_inc_totals:
        if show_details:
            html = "<h1> DividendDB Report for %4d excluding Trading a/cs with investment Details</h1>" % (year)
        else:
            html = "<h1> DividendDB Report for %4d excluding Trading a/cs as Summary Only</h1>" % (year)
    else:
        if show_details:
            html = "<h1> DividendDB Report for %4d including Trading a/cs with investment Details</h1>" % (year)
        else:
            html = "<h1> DividendDB Report for %4d including Trading a/cs as Summary Only</h1>" % (year)
     
    html += "<h2> Combined Dividends </h2>"
    
    html += display_combined_divs(year, include_current_month, include_markets, use_inc_totals)
        
    html += "<h2>PortfolioDB Dividends</h2>"
    
    portfolios = get_portfolios(database = database, "%", use_inc_totals)
    
    for portfolio in portfolios:
        por_name = portfolio[0]
        html += display_portfolio_divs(por_name, year, include_current_month, include_markets, use_inc_totals, show_details)
    
    f.write(html)
    finish_doc(f)
    f.close()
    
    html_link = 'file://localhost/%s' % (fname)
    webbrowser.open(html_link)

    return
 
    
if __name__ == "__main__":
    
    print("Started report_div_annual")
    print(" ")
    print("Open Database")
        
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)   
    
    include_current_month = True
    include_markets       = True
    use_inc_totals        = False
    update_exd            = True
    show_details          = True
    year                  = 2015

    print("report for %s" % (year))
    report_div_annual(database              = database,
                      year                  = year, 
                      include_current_month = include_current_month,  
                      include_markets       = include_markets, 
                      update_exd            = update_exd, 
                      use_inc_totals        = use_inc_totals, 
                      show_details          = show_details)

    print("Close database")
    close_db( database = database)   
    
    print("Finished report_div_annual") 
    
    
 
