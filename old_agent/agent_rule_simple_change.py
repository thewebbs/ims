#------------------------------------------------------------
# Filename  : agent_rule_simple_change.py
# Project   : ava
#
# Descr     : This file contains the agent_rule_simple_change class and methods
#
# Params    : None
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
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from collections import deque
from utils.config import RULE_PERCENT_DECREASE, RULE_VALUE_INCREASE

#
# Delete methods
#


#
# Get methods
#

def get_next_action(self):
    
    return self.next_action


#
# Set methods
#

def add_new_item(self, next_value):
    
    # because they're set up using collection deque it automatically keeps it to the max length, deleting the oldest
    self.value_collection.append(next_value)
    
    set_next_action(self, next_value)   # work out the action BEFORE setting the new max and min
    
    set_max_and_min_and_len(self)
    
    #print(self.value_collection)
    if self.next_action != 'NOTHING' and self.prev_action != 'NOTHING':
        if self.prev_action != self.next_action:
            print('//////////////adding', next_value, 'max',self.max_value_in_list, 'min',self.min_value_in_list, 'next_action',self.next_action, 'prev_action',self.prev_action)
    
    return 

def set_max_and_min_and_len(self):
    
    self.max_value_in_list = max(self.value_collection)
    self.min_value_in_list = min(self.value_collection)
    self.len_of_list       = len(self.value_collection)
        
    return 

def set_next_action(self, next_value):
    
    this_change_value = next_value - self.max_value_in_list  
    if this_change_value > 0:  
        if self.max_value_in_list != 0:
            this_change_percent = abs((this_change_value / self.max_value_in_list) * 100)
        else:
            this_change_percent = 0
    else:
        #this_change_percent = 0
        this_change_percent = abs((this_change_value / self.max_value_in_list) * 100)
    #print('this_change_value',this_change_value,'this_change_percent',this_change_percent)
   

    if self.len_of_list > 1:
        
        if this_change_value > 0:
            # Price has gone up
            if this_change_value > RULE_VALUE_INCREASE:
                # Price has gone up a lot!
                self.prev_action = self.next_action
                self.next_action = 'SELL'
            else:
                # not huge difference so ignore
                self.next_action = 'NOTHING'
                
        
        else:
            # Price has gone down
            if this_change_percent > RULE_PERCENT_DECREASE:
                # Price has dropped a lot!
                self.prev_action = self.next_action
                self.next_action = 'BUY'
            else:
                # not huge difference so ignore
                self.next_action = 'NOTHING'
    else:
        self.next_action = 'NOTHING'
                
    return 

    

#
# Class definition
#

class Agent_rule_simple_change:

    def __init__(self, num_items):

        self.num_items   = num_items
        self.value_collection = deque(maxlen = num_items)
        self.max_value_in_list = 0
        self.min_value_in_list = 0
        self.len_of_list = 0
        self.next_action = ''
        self.last_action = ''
        
        return
    

    def __repr__(self):
        
        return self.num_items + self.value_collection + self.max_value_in_list + self.min_value_in_list + self.len_of_list + self.next_action + self.last_action
    
    
    def __str__(self): 
        
        return self.num_items + self.value_collection + self.max_value_in_list + self.min_value_in_list + self.len_of_list + self.next_action + self.last_action


    def __unicode__(self): 
        
        return self.num_items + self.value_collection + self.max_value_in_list + self.min_value_in_list + self.len_of_list + self.next_action + self.last_action
        