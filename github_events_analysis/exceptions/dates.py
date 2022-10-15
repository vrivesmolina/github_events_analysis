""" This script contains all the custom exceptions related to dates """
import logging


class NotValidDatesException(Exception):
    """Exception raised when the input final day is a later date to the
    input start day"""
    def __init__(self, initial_day: int, last_day: int):
        logging.error(
            f"The final_day ({last_day}) can't be a previous date "
            f"to the initial_day ({initial_day})"
        )
