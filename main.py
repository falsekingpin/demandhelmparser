# -*- coding: utf-8 -*-

from __future__ import print_function
__author__ = "Akshay Nar"

#importing dependencies
from utils.constants import Constants
from utils.utils import Utils

# other dependencies
import logging
import datetime
import sys

class DataProcessing:
    """
    @Definition:
    This class is used for data processing
    Process Flow:
    1. Getting input which flow to run(get_top_rated_author,sort_by_comments)
    2. Create request for hitting the hackernews endpoint for top page and authors in top page
    3. Hit the enpoints and get response
    4. For every response process the data and generate metrics

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
        #global variables
        self.input = sys.argv[1]

    def process_logic(self):
        """
        @Definition:
        This function has the process logic for execution of the process flow
        """
        try:
            start_time = datetime.datetime.now()

            self.logger.info("DP : input : {}".format(self.input))
            print(self.input)

            subtext_data = self.initiate_processing()
            self.logger.info("DP : process_logic : subtext_data : {}".format(subtext_data))

            if(self.input == Constants.GET_TOP_RATED_AUTHOR):
                
                subtext_df = Utils().create_df(subtext_data)

                self.logger.info("DP : process_logic : subtext_df : {}".format(subtext_df))

                Utils().print_prettify_df(subtext_df)
            elif(self.input == Constants.SORT_BY_COMMENTS):
                story_data = self.get_story_data()

                sorted_df = self.sort_data(subtext_data,story_data)

                Utils().print_prettify_df(sorted_df)

            end_time = datetime.datetime.now()
            self.logger.info("DP : Total Time Taken : {}".format(end_time - start_time))
        except Exception as error:
            self.logger.error("DP : ERROR : {}".format(error))
        finally:
            self.logger.info("DP : Processing Done")

    def initiate_processing(self):
        """
        @Definition:
        This function is used for calling the api for hackernews html and sending it for processing using bs4

        @return:
        subtext_data: returns subtext data
        """
        try:
            response = Utils().call_api(Constants.URL)

            soup_data = Utils().get_soup_object(response)

            post_data = Utils().find_all_data_in_soup_object(soup_data,Constants.TD,Constants.CLASS,Constants.SUBTEXT)

            subtext_data = []

            for data in post_data:
                subtext_dict = self.prepare_subtext_dict(data)
                subtext_data.append(subtext_dict)

            self.logger.info("DI : initiate_processing : subtext_dict : {}".format(subtext_dict))

            subtext_data.append(subtext_dict)

            return subtext_data
        except Exception as error:
            self.logger.error("DP : ERROR : initiate_processing : {}".format(error))
            pass

    def prepare_subtext_dict(self,subtext_data):
        """
        @Definition:
        This function is used for creating subtext data i.e what appears below a author i.e its comments, time posted

        @params:
        subtext_data: subtext data is the subtext html from the page

        @return:
        subtext_dict: subtext_dict contains keys and values of subtext html
        """
        try:
            subtext_dict = {}

            subtext_dict[Constants.ID] = int(((subtext_data.contents[1][Constants.ID]).split("_"))[1])

            subtext_dict[Constants.POINTS] = int(((subtext_data.contents[1].text).split(" "))[0])

            subtext_dict[Constants.AUTHOR] = subtext_data.contents[3].text

            karma_points = self.get_karma_points_for_author(subtext_data.contents[3].text)

            subtext_dict[Constants.KARMA_POINTS] = karma_points

            subtext_dict[Constants.TIME_POSTED] = int(((subtext_data.contents[5].text).split(" "))[0])

            comments = ((subtext_data.contents[11].text).split("\xa0"))[0]
            
            if(comments == Constants.DISCUSS):
                comments = 0
            
            subtext_dict[Constants.COMMENTS] = int(comments)

            return subtext_dict
        except Exception as error:
            self.logger.error("DP : ERROR : prepare_subtext_dict : {}".format(error))
            pass


    def get_karma_points_for_author(self,author):
        """
        @Definition:
        This function is used for getting karma points for the author

        @params:
        author: author is the author name

        @return:
        karma_points: returns karma points for the author
        """
        try:
            url = Constants.KARMA_POINTS_URL + author
            response = Utils().call_api(url)

            karma_data = Utils().get_soup_object(response)
            filtered_karma_data = Utils().find_all_data_in_soup_object_without_attr(karma_data,Constants.TD)

            karma_points_uf = filtered_karma_data[10].text
            karma_points = karma_points_uf.replace(' ','')
            
            self.logger.info("DP : get_karma_points_for_author : karma_points : {}".format(karma_points))

            return int(karma_points)
        except Exception as error:
            self.logger.error("DP : ERROR : get_karma_points_for_author : {}".format(error))

    def get_story_data(self):
        """
        @Definition:
        This function is used for fetching the story data

        @return:
        story_list: returns a list of stories i.e its html
        """
        try:
            response = Utils().call_api(Constants.URL)
            soup_data = Utils().get_soup_object(response)

            author_data = Utils().find_all_data_in_soup_object(soup_data,Constants.TR,Constants.CLASS,Constants.ATHING)

            story_list = []

            for data in author_data:
                story_dict = {}
                story_dict[Constants.STORY] = data.contents[4].text
                story_dict[Constants.ID] = int(data[Constants.ID])
                
                self.logger.info("DP : get_story_data : story_dict : {}".format(story_dict))

                story_list.append(story_dict)

            return story_list
        except Exception as error:
            self.logger.error("DP : get_story_data : ERROR : {}".format(error))
            pass

    def sort_data(self,subtext_data,story_data):
        """
        @Definition:
        This function is used for sorting the data

        @params:
        subtext_data: subtext data conatins the list of subtext elements
        story_data: story data contains the list of story elements

        @return:
        sorted_df: returns sorted_df which is obtained by combining both subtext_df and story_df
        """

        try:
            subtext_df = Utils().create_df(subtext_data)
            story_df = Utils().create_df(story_data)

            joined_df = Utils().join_df(subtext_df,story_df,on_param=Constants.ID,how_param=Constants.OUTER)

            self.logger.info("DP : sort_data : joined_df : {}".format(joined_df))

            sorted_df = Utils().sort_df(joined_df,column_name=Constants.COMMENTS)

            self.logger.info("DP : sort_data : sorted_df : {}".format(sorted_df))

            return sorted_df
        except Exception as error:
            self.logger.error("DP : sort_data : ERROR : {}".format(error))
            pass

"""
____________
    
calling main 
____________

"""
if __name__ == "__main__":
    DataProcessing().process_logic()