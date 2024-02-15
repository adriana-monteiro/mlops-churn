"""
This is a boilerplate pipeline
generated using Kedro 0.18.8
"""

import logging
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd
import great_expectations as ge

from .utils import one_hot_check

def unit_test_after_preprocessing(
    data_eng: pd.DataFrame,
    data_enc: pd.DataFrame
): 
    
    ## verifying until feature engineer
   columns_eng = ['IsCanceled', 'LeadTime','ArrivalDateYear', 'ArrivalDateMonth', 'ArrivalDateWeekNumber', 'ArrivalDateDayOfMonth', 
               'StaysInWeekendNights', 'StaysInWeekNights', 'Adults','Children', 'Babies', 'Meal', 'Country', 'MarketSegment', 
               'DistributionChannel', 'IsRepeatedGuest', 'PreviousCancellations', 'PreviousBookingsNotCanceled','ReservedRoomType', 
               'AssignedRoomType', 'BookingChanges', 'DepositType', 'Agent', 'Company', 'DaysInWaitingList', 'CustomerType', 'ADR', 
               'RequiredCarParkingSpaces', 'TotalOfSpecialRequests', 'ReservationStatus', 'ReservationStatusDate', 'ArrivalDate', 
               'TotalGuests', 'WithBabies', 'WithChildren', 'AdultsOnly', 'IsFamily', 'IsTwo', 'IsGroup', 'TotalLengthOfStay',
               'Season', 'Semester', 'Quarter', 'CancellationRate', 'IsNewBooking', 'HasSpecialRequests']
    
   pd_df_ge_eng = ge.from_pandas(data_eng)

    # check if all columns are in df
   assert pd_df_ge_eng.expect_table_columns_to_match_ordered_list(columns_eng).success == True

   #  # check if datatypes are correct
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("IsCanceled", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("LeadTime", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("ArrivalDateYear", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("ArrivalDateMonth", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("ArrivalDateWeekNumber", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("ArrivalDateDayOfMonth", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("StaysInWeekendNights", "float64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("StaysInWeekNights", "float64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("Adults", "float64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("Children", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("Babies", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("Meal", "object").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("Country", "object").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("MarketSegment", "object").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("DistributionChannel", "object").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("IsRepeatedGuest", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("PreviousCancellations", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("PreviousBookingsNotCanceled", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("ReservedRoomType", "object").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("AssignedRoomType", "object").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("BookingChanges", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("Agent", "object").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("DepositType", "object").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("Company", "object").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("DaysInWaitingList", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("CustomerType", "object").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("ADR", "float64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("RequiredCarParkingSpaces", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("TotalOfSpecialRequests", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("ReservationStatus", "object").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("ReservationStatusDate", "object").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("ArrivalDate", "object").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("TotalGuests", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("WithBabies", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("WithChildren", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("AdultsOnly", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("IsFamily", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("IsTwo", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("IsGroup", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("TotalLengthOfStay", "float64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("Season", "object").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("Semester", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("Quarter", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("CancellationRate", "float64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("IsNewBooking", "int64").success == True
   assert pd_df_ge_eng.expect_column_values_to_be_of_type("HasSpecialRequests", "int64").success == True

   for column in columns_eng:
      assert pd_df_ge_eng.expect_column_values_to_not_be_null(column).success == True


   #  # there should be less cancellations than non cancellations
   assert (data_eng['IsCanceled'].value_counts()[0] > data_eng['IsCanceled'].value_counts()[1]) == True
    
   #  # categories
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='IsCanceled', value_set=[0, 1]).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='ArrivalDateMonth', value_set=[1,2,3,4,5,6,7,8,9,10,11,12]).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='Meal', value_set=['BB       ', 'FB       ', 'HB       ', 'SC       ']).success == True
   assert pd_df_ge_eng.expect_column_values_to_match_regex(column='Country', regex=r'^[A-Z]{2}$',mostly=1) # matching three letter country code
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='MarketSegment', value_set=['Offline TA/TO', 'Online TA', 'Groups', 'Complementary', 'Direct',
        'Corporate', 'Undefined', 'Aviation']).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='DistributionChannel', value_set=['TA/TO', 'Direct', 'Undefined', 'Corporate', 'GDS']).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='IsRepeatedGuest', value_set=[0, 1]).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='ReservedRoomType', value_set=['A               ', 'B               ', 'D               ',
        'F               ', 'E               ', 'G               ',
        'C               ', 'P               ']).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='AssignedRoomType', value_set=['A               ', 'B               ', 'F               ',
        'D               ', 'G               ', 'E               ',
        'K               ', 'C               ', 'P               ']).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='DepositType', value_set=['No Deposit     ', 'Non Refund     ', 'Refundable     ']).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='CustomerType', value_set=['Transient', 'Transient-Party', 'Contract', 'Group']).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='ReservationStatus', value_set=['Check-Out', 'Canceled', 'No-Show']).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='WithBabies', value_set=[0, 1]).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='WithChildren', value_set=[0, 1]).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='AdultsOnly', value_set=[0, 1]).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='IsFamily', value_set=[0, 1]).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='IsTwo', value_set=[0, 1]).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='IsGroup', value_set=[0, 1]).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='Season', value_set=['Summer', 'Autumn', 'Winter', 'Spring']).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='Semester', value_set=[1, 2]).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='Quarter', value_set=[1, 2, 3, 4]).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='IsNewBooking', value_set=[0, 1]).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_in_set( column='HasSpecialRequests', value_set=[0, 1]).success == True

    
   #  # minimum values
   assert pd_df_ge_eng.expect_column_min_to_be_between(column='LeadTime', min_value=0, max_value=0).success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column='ArrivalDateYear', min_value=2015, max_value=2015).success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column='StaysInWeekendNights', min_value=0, max_value=0).success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column='StaysInWeekNights', min_value=0, max_value=0).success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column='Adults', min_value=1, max_value=1).success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column='Children', min_value=0, max_value=0).success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column='Babies', min_value=0, max_value=0).success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column='PreviousCancellations', min_value=0, max_value=0).success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column='PreviousBookingsNotCanceled', min_value=0, max_value=0).success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column="BookingChanges",  min_value=0, max_value=0).success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column="DaysInWaitingList",  min_value=0, max_value=0).success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column="RequiredCarParkingSpaces",  min_value=0, max_value=0).success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column="TotalOfSpecialRequests",  min_value=0, max_value=0).success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column='ReservationStatusDate', min_value='2014-01-01', max_value='2014-12-31').success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column='ArrivalDate', min_value='2015-07-01', max_value='2015-12-31').success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column='TotalGuests', min_value=1, max_value=1).success == True
   assert pd_df_ge_eng.expect_column_min_to_be_between(column='TotalLengthOfStay', min_value=1, max_value=1).success == True

   #  # range
   assert pd_df_ge_eng.expect_column_values_to_be_between(column='ArrivalDateWeekNumber', min_value=1, max_value=53).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_between(column='ArrivalDateDayOfMonth', min_value=1, max_value=31).success == True
   assert pd_df_ge_eng.expect_column_values_to_be_between(column='CancellationRate', min_value=0, max_value=1).success == True

   #  # data inconsistencies
   assert data_eng[(data_eng['IsCanceled'] == 0) & (data_eng['ReservationStatus'] == 'Canceled')].empty == True
   assert data_eng[(data_eng['IsCanceled'] == 1) & (data_eng['ReservationStatus'] == 'Check-Out')].empty == True
   assert data_eng.loc[data_eng['ADR'] <= 0, 'ADR'].empty == True
   assert data_eng.loc[data_eng['Adults'] == 0, 'Adults'].empty == True 
   assert data_eng.loc[(data_eng['StaysInWeekendNights'] + data_eng['StaysInWeekNights']) <= 0, ['StaysInWeekendNights', 'StaysInWeekNights']].empty == True

   # verifying if one hot encoding was done correctly

   columns_enc = ['Agent_1', 'Agent_2', 'Agent_3', 'Agent_4', 'Agent_6', 'Agent_7', 'Agent_8', 'Agent_9', 'Agent_10', 'Agent_11',
                  'Agent_12', 'Agent_13', 'Agent_14', 'Agent_15', 'Agent_16', 'Agent_17', 'Agent_19', 'Agent_20', 'Agent_21',
                  'Agent_22', 'Agent_23', 'Agent_26', 'Agent_27', 'Agent_28', 'Agent_29', 'Agent_30', 'Agent_31', 'Agent_32',
                  'Agent_33', 'Agent_34', 'Agent_35', 'Agent_37', 'Agent_38', 'Agent_39', 'Agent_40', 'Agent_42', 'Agent_44', 
                  'Agent_45', 'Agent_50', 'Agent_52', 'Agent_53', 'Agent_56', 'Agent_57', 'Agent_58', 'Agent_66', 'Agent_71',
                  'Agent_75', 'Agent_77', 'Agent_79', 'Agent_81', 'Agent_82', 'Agent_83', 'Agent_85', 'Agent_86', 'Agent_87',
                  'Agent_89', 'Agent_91', 'Agent_94', 'Agent_98', 'Agent_99', 'Agent_103', 'Agent_104', 'Agent_111', 'Agent_118',
                  'Agent_119', 'Agent_121', 'Agent_128', 'Agent_129', 'Agent_132', 'Agent_133', 'Agent_134', 'Agent_138', 'Agent_147', 
                  'Agent_151', 'Agent_152', 'Agent_153', 'Agent_154', 'Agent_155', 'Agent_157', 'Agent_159', 'Agent_168', 'Agent_171', 
                  'Agent_173', 'Agent_174', 'Agent_177', 'Agent_179', 'Agent_182', 'Agent_187', 'Agent_191', 'Agent_192', 'Agent_195', 
                  'Agent_205', 'Agent_210', 'Agent_214', 'Agent_215', 'Agent_219', 'Agent_220', 'Agent_229', 'Agent_234', 'Agent_240', 
                  'Agent_254', 'Agent_262', 'Agent_281', 'Agent_290', 'Agent_296', 'Agent_315', 'Agent_330', 'Agent_341', 'Agent_355',
                  'Agent_378', 'Agent_390', 'Agent_394', 'Agent_423', 'Agent_425', 'Agent_484', 'Agent_509','Agent_NULL', 'ArrivalDateMonth_1', 
                  'ArrivalDateMonth_2', 'ArrivalDateMonth_3', 'ArrivalDateMonth_4', 'ArrivalDateMonth_5', 'ArrivalDateMonth_6', 'ArrivalDateMonth_7', 
                  'ArrivalDateMonth_8', 'ArrivalDateMonth_9', 'ArrivalDateMonth_10', 'ArrivalDateMonth_11', 'ArrivalDateMonth_12', 
                  'CustomerType_Contract', 'CustomerType_Group', 'CustomerType_Transient', 'CustomerType_Transient-Party', 'DepositType_NoDeposit', 
                  'DepositType_NonRefund', 'DepositType_Refundable', 'DistributionChannel_Corporate', 'DistributionChannel_Direct', 
                  'DistributionChannel_GDS', 'DistributionChannel_TA/TO', 'MarketSegment_Aviation', 'MarketSegment_Complementary', 
                  'MarketSegment_Corporate', 'MarketSegment_Direct', 'MarketSegment_Groups', 'MarketSegment_OfflineTA/TO', 'MarketSegment_OnlineTA', 
                  'Meal_BB', 'Meal_FB', 'Meal_HB', 'Meal_SC', 'ReservedRoomType_A', 'ReservedRoomType_B', 'ReservedRoomType_C', 'ReservedRoomType_D', 
                  'ReservedRoomType_E', 'ReservedRoomType_F', 'ReservedRoomType_G', 'Season_Autumn', 'Season_Spring', 'Season_Summer', 'Season_Winter', 
                  'Semester_1', 'Semester_2', 'Quarter_1', 'Quarter_2', 'Quarter_3', 'Quarter_4', 'LeadTime', 'ArrivalDateYear', 'ArrivalDateWeekNumber', 
                  'ArrivalDateDayOfMonth', 'StaysInWeekendNights', 'StaysInWeekNights', 'Adults', 'Children', 'Babies', 'PreviousCancellations', 
                  'PreviousBookingsNotCanceled', 'DaysInWaitingList', 'ADR', 'RequiredCarParkingSpaces', 'TotalOfSpecialRequests', 'TotalGuests', 
                  'TotalLengthOfStay', 'CancellationRate', 'IsCanceled', 'IsRepeatedGuest', 'WithChildren', 'WithBabies', 'AdultsOnly', 'IsFamily', 
                  'IsTwo', 'IsGroup', 'IsNewBooking', 'HasSpecialRequests']

   pd_df_ge_enc = ge.from_pandas(data_enc)

    # check if all columns are in df
   assert pd_df_ge_enc.expect_table_columns_to_match_ordered_list(columns_enc).success == True

   # check if one hot encoded was performed correctly
   one_hot_check(data_eng, data_enc,'Agent')
   one_hot_check(data_eng, data_enc,'ArrivalDateMonth')
   one_hot_check(data_eng, data_enc,'CustomerType')
   one_hot_check(data_eng, data_enc,'DepositType')
   one_hot_check(data_eng, data_enc,'DistributionChannel')
   one_hot_check(data_eng, data_enc,'MarketSegment')
   one_hot_check(data_eng, data_enc,'Meal')
   one_hot_check(data_eng, data_enc,'ReservedRoomType')
   one_hot_check(data_eng, data_enc,'Season')
   one_hot_check(data_eng, data_enc,'Semester')
   one_hot_check(data_eng, data_enc,'Quarter')


   log = logging.getLogger(__name__)
   log.info("Data passed on the unit data tests after preprocessing")




   return 0

