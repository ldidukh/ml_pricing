"""
    Module for the reward functions aka as profit or revenue

"""
import numpy as np
#import wrapper for abstract functions

def reward_function():
    """
        abstract function for wrapping different kind of rewards
    """
    return

def profit(price, cost, quantity):
    """_summary_

    Args:
        price (_type_): _description_
        cost (_type_): _description_
        quantity (_type_): _description_
    """
    profit_value = quantity*(price-cost)
    return profit_value

def revenue(price, quantity):
    """_summary_

    Args:
        price (_type_): _description_
        cost (_type_): _description_
        quantity (_type_): _description_
    """
    revenue_value = price*quantity
    return revenue_value