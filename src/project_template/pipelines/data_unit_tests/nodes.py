"""
This is a boilerplate pipeline
generated using Kedro 0.18.8
"""

import logging
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd
import great_expectations as ge

def unit_test_before_preprocessing(
    data: pd.DataFrame,
): 
    
    pd_df_ge = ge.from_pandas(data)
    columns = ['IsCanceled', 'LeadTime', 'ArrivalDateYear', 'ArrivalDateMonth', 'ArrivalDateWeekNumber', 
               'ArrivalDateDayOfMonth', 'StaysInWeekendNights', 'StaysInWeekNights', 'Adults', 'Children', 
               'Babies', 'Meal', 'Country', 'MarketSegment', 'DistributionChannel', 'IsRepeatedGuest', 
               'PreviousCancellations', 'PreviousBookingsNotCanceled', 'ReservedRoomType', 'AssignedRoomType', 
               'BookingChanges', 'DepositType', 'Agent', 'Company', 'DaysInWaitingList', 'CustomerType', 
               'ADR', 'RequiredCarParkingSpaces', 'TotalOfSpecialRequests', 'ReservationStatus', 'ReservationStatusDate']

    # check if all columns are in df
    assert pd_df_ge.expect_table_columns_to_match_ordered_list(columns).success == True

    # check if datatypes are correct
    assert pd_df_ge.expect_column_values_to_be_of_type("IsCanceled", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("LeadTime", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("ArrivalDateYear", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("ArrivalDateMonth", "object").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("ArrivalDateWeekNumber", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("ArrivalDateDayOfMonth", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("StaysInWeekendNights", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("StaysInWeekNights", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("Adults", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("Children", "float").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("Babies", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("Meal", "object").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("Country", "object").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("MarketSegment", "object").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("DistributionChannel", "object").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("IsRepeatedGuest", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("PreviousCancellations", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("PreviousBookingsNotCanceled", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("ReservedRoomType", "object").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("AssignedRoomType", "object").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("BookingChanges", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("DepositType", "object").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("Agent", "object").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("Company", "object").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("DaysInWaitingList", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("CustomerType", "object").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("ADR", "float").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("RequiredCarParkingSpaces", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("TotalOfSpecialRequests", "int").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("ReservationStatus", "object").success == True
    assert pd_df_ge.expect_column_values_to_be_of_type("ReservationStatusDate", "object").success == True
    
    # check for not expected null values
    assert pd_df_ge.expect_column_values_to_not_be_null("IsCanceled").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("LeadTime").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("ArrivalDateYear").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("ArrivalDateMonth").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("ArrivalDateWeekNumber").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("ArrivalDateDayOfMonth").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("StaysInWeekendNights").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("StaysInWeekNights").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("Adults").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("Babies").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("MarketSegment").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("DistributionChannel").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("IsRepeatedGuest").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("PreviousCancellations").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("PreviousBookingsNotCanceled").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("ReservedRoomType").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("AssignedRoomType").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("BookingChanges").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("DepositType").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("Agent").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("Company").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("DaysInWaitingList").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("CustomerType").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("ADR").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("RequiredCarParkingSpaces").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("TotalOfSpecialRequests").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("ReservationStatus").success == True
    assert pd_df_ge.expect_column_values_to_not_be_null("ReservationStatusDate").success == True

   # less than 2% null values brefore preprocessing
    assert data.isnull().sum().sum()/len(data) < 0.02

    # there should be less cancellations than non cancellations
    assert (data['IsCanceled'].value_counts()[0] > data['IsCanceled'].value_counts()[1]) == True
    
    # categories
    assert pd_df_ge.expect_column_values_to_be_in_set( column='IsCanceled', value_set=[0, 1]).success == True
    assert pd_df_ge.expect_column_values_to_be_in_set( column='ArrivalDateMonth', value_set=['April', 'August', 'December', 'February', 'January', 'July', 'June', 'March', 'May', 'November', 'October', 'September']).success == True
    assert pd_df_ge.expect_column_values_to_be_in_set( column='Meal', value_set=['BB       ', 'FB       ', 'HB       ', 'SC       ']).success == True
    assert pd_df_ge.expect_column_values_to_match_regex(column='Country', regex=r'^[A-Z]{2}$',mostly=1) # matching three letter country code
    assert pd_df_ge.expect_column_values_to_be_in_set( column='MarketSegment', value_set=['Offline TA/TO', 'Online TA', 'Groups', 'Complementary', 'Direct',
       'Corporate', 'Undefined', 'Aviation']).success == True
    assert pd_df_ge.expect_column_values_to_be_in_set( column='DistributionChannel', value_set=['TA/TO', 'Direct', 'Undefined', 'Corporate', 'GDS']).success == True
    assert pd_df_ge.expect_column_values_to_be_in_set( column='IsRepeatedGuest', value_set=[0, 1]).success == True
    assert pd_df_ge.expect_column_values_to_be_in_set( column='ReservedRoomType', value_set=['A               ', 'B               ', 'D               ',
       'F               ', 'E               ', 'G               ',
       'C               ', 'P               ']).success == True
    assert pd_df_ge.expect_column_values_to_be_in_set( column='AssignedRoomType', value_set=['A               ', 'B               ', 'F               ',
       'D               ', 'G               ', 'E               ',
       'K               ', 'C               ', 'P               ']).success == True
    assert pd_df_ge.expect_column_values_to_be_in_set( column='DepositType', value_set=['No Deposit     ', 'Non Refund     ', 'Refundable     ']).success == True
    assert pd_df_ge.expect_column_values_to_be_in_set( column='CustomerType', value_set=['Transient', 'Transient-Party', 'Contract', 'Group']).success == True
    assert pd_df_ge.expect_column_values_to_be_in_set( column='ReservationStatus', value_set=['Check-Out', 'Canceled', 'No-Show']).success == True

    # minimum values
    assert pd_df_ge.expect_column_min_to_be_between(column='LeadTime', min_value=0, max_value=0).success == True
    assert pd_df_ge.expect_column_min_to_be_between(column='ArrivalDateYear', min_value=2015, max_value=2015).success == True
    assert pd_df_ge.expect_column_min_to_be_between(column='StaysInWeekendNights', min_value=0, max_value=0).success == True
    assert pd_df_ge.expect_column_min_to_be_between(column='StaysInWeekNights', min_value=0, max_value=0).success == True
    assert pd_df_ge.expect_column_min_to_be_between(column='Adults', min_value=0, max_value=0).success == True
    assert pd_df_ge.expect_column_min_to_be_between(column='Children', min_value=0, max_value=0).success == True
    assert pd_df_ge.expect_column_min_to_be_between(column='Babies', min_value=0, max_value=0).success == True
    assert pd_df_ge.expect_column_min_to_be_between(column='PreviousCancellations', min_value=0, max_value=0).success == True
    assert pd_df_ge.expect_column_min_to_be_between(column='PreviousBookingsNotCanceled', min_value=0, max_value=0).success == True
    assert pd_df_ge.expect_column_min_to_be_between("BookingChanges",  min_value=0, max_value=0).success == True
    assert pd_df_ge.expect_column_min_to_be_between("DaysInWaitingList",  min_value=0, max_value=0).success == True
    assert pd_df_ge.expect_column_min_to_be_between("RequiredCarParkingSpaces",  min_value=0, max_value=0).success == True
    assert pd_df_ge.expect_column_min_to_be_between("TotalOfSpecialRequests",  min_value=0, max_value=0).success == True
    assert pd_df_ge.expect_column_min_to_be_between(column='ReservationStatusDate', min_value='2014-01-01', max_value='2014-12-31').success == True

    # range
    assert pd_df_ge.expect_column_values_to_be_between(column='ArrivalDateWeekNumber', min_value=1, max_value=53).success == True
    assert pd_df_ge.expect_column_values_to_be_between(column='ArrivalDateDayOfMonth', min_value=1, max_value=31).success == True

    # data inconsistencies
    assert data[(data['IsCanceled'] == 0) & (data['ReservationStatus'] == 'Canceled')].empty == True
    assert data[(data['IsCanceled'] == 1) & (data['ReservationStatus'] == 'Check-Out')].empty == True


    log = logging.getLogger(__name__)
    log.info("Data passed on the unit data tests before preprocessing")
    
    return 0