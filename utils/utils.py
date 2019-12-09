# -*- coding: utf-8 -*-

from __future__ import print_function
__author__ = "Akshay Nar"

# importing dependencies
import logging
import requests
import pandas as pd

# importing other dependencies
from .constants import Constants
from bs4 import BeautifulSoup
from IPython.display import display, HTML

class Utils:
    """
    @Definition:
    Utils class is used for providing utilitiy methods

    """
    # for logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # create a file handler
    handler = logging.FileHandler('/tmp/dataProcessing.log')
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter(
        '%(levelname)-8s %(asctime)s,%(msecs)d  [%(filename)s:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    """
    ____________
    
    init method
    ____________
    """
    
    def __init__(self):
        """
        @Definition:
        Used for initializing constant variables
        """

        pass

    def call_api(self,url):
        """
        @Definition:
        This function is used for calling endpoint and returning HTML content

        @return:
        response: returns HTML content
        """
        try:
            response = requests.get(url)

            self.logger.info("UTILS : call_api : {}".format(response))
            return response

        except Exception as error:
            self.logger.error("UTILS : ERROR : call_api :{}".format(error))

    def get_soup_object(self,data):
        """
        @Definition:
        This function is used for getting the soup object

        @return:
        soup_data: returns soup_data i.e soup object
        """
        try:
            soup_data = BeautifulSoup(data.content, 'html5lib')

            self.logger.info("UTILS : get_soup_object : {}".format(soup_data))

            return soup_data
        except Exception as error:
            self.logger.error("UTILS : ERROR : get_soup_object :{}".format(error))

    def find_all_data_in_soup_object(self,soup_data,tag,attr_key,attr_value):
        """
        @Definition:
        This function is used for finding all data in soup object

        @params:
        soup_data: soup_data contains the soup object
        tag: tag contains the tag of html
        attr_key: attr_key is the atrribute_key for tag
        attr_value: attr_value is the attribute value for tag

        @return:
        filtered_data: returns filtered_data which is the filtered soup object
        """
        try:
            filtered_data = soup_data.findAll(tag, attrs = {attr_key:attr_value})

            self.logger.info("UTILS : find_data_in_soup_object : {}".format(filtered_data))

            return filtered_data
        except Exception as error:
            self.logger.error("UTILS : ERROR : find_data_in_soup_object :{}".format(error))

    def find_all_data_in_soup_object_without_attr(self,soup_data,tag):
        """
        @Definition:
        This function is used for finding all data in soup object

        @params:
        soup_data: soup_data contains the soup object
        tag: tag contains the tag of html

        @return:
        filtered_data: returns filtered_data which is the filtered soup object
        """
        try:
            filtered_data = soup_data.findAll(tag)

            self.logger.info("UTILS : find_data_in_soup_object_without_attr : {}".format(filtered_data))

            return filtered_data
        except Exception as error:
            self.logger.error("UTILS : ERROR : find_data_in_soup_object_without_attr :{}".format(error))

    def create_df(self,processed_data):
        """
        @Definition:
        This function is used for creating the df from a list of dictionaries

        @param:
        processed_data: processed_data is the list of dictionaries

        @return:
        data_df: returns df created from the processed_data
        """
        try:
            data_df = pd.DataFrame(processed_data)

            self.logger.info("UTILS : create_df : {}".format(data_df))

            return data_df
        except Exception as error:
            self.logger.error("UTILS : ERROR : create_df :{}".format(error))

    def join_df(self,df1,df2,on_param,how_param):
        """
        @Definition:
        This function is used for joining two df

        @param:
        df1: df1 data
        df2: df2 data
        on_param: on param is the column on which the join is to be performed
        how_param: how_param is what kind of join we have to perform

        @return:
        joined_df: returns joined_df from two input df
        """
        try:
            joined_df = pd.merge(df1, df2, on=on_param, how=how_param)

            self.logger.info("PD : UTILS : join_df : {}".format(joined_df))

            return joined_df
        except Exception as error:
            self.logger.error("UTILS : ERROR : join_df :{}".format(error))

    def sort_df(self,data_df,column_name):
        """
        @Definition:
        This function is used for sorting the df

        @param:
        data_df: has processed_data
        column_data: on which column the data is to be sorted

        @return:
        sorted_df: sorted_df contains sorted data in df
        """
        try:
            sorted_df = data_df.sort_values(by=column_name)

            self.logger.info("PD : UTILS : sort_df : {}".format(sorted_df))

            return sorted_df
        except Exception as error:
            self.logger.error("UTILS : ERROR : sort_df :{}".format(error))

    def print_prettify_df(self,data_df):
        """
        @Definition:
        This function is used for printing the df

        @param:
        data_df: df which contains processed data
        
        """
        try:
            print(data_df)
            display(HTML(data_df.to_html()))
        except Exception as error:
            self.logger.error("UTILS : ERROR : create_df :{}".format(error))