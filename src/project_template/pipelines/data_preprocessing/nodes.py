"""
This is a boilerplate pipeline
generated using Kedro 0.18.8
"""

import logging
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd
from .utils import *
import re

from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import OneHotEncoder , LabelEncoder, OrdinalEncoder
import shap 
import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score, classification_report
from sklearn.ensemble import RandomForestClassifier
import sklearn
import mlflow
from sklearn.preprocessing import StandardScaler

def clean_data(
    data: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict, Dict]:
    """Does dome data cleaning.
    Args:
        data: Data containing features and target.
    Returns:
        data: Cleaned data
    """
    df = data.copy()

    # 1. ADR (Average Daily Rate)
    df.loc[df['ADR'] <= 0, 'ADR'] = np.nan

    # 2. Adults
    df.loc[df['Adults'] == 0, 'Adults'] = np.nan

    # 3. IsCanceled, ReservationStatus
    df.loc[(df['IsCanceled'] == 0) & (df['ReservationStatus'] == 'Canceled'), 'ReservationStatus'] = np.nan
    df.loc[(df['IsCanceled'] == 1) & (df['ReservationStatus'] == 'Check-Out'), 'ReservationStatus'] = np.nan

    # 4. StaysInWeekendNights, StaysInWeekNights
    df.loc[(df['StaysInWeekendNights'] + df['StaysInWeekNights']) <= 0, ['StaysInWeekendNights', 'StaysInWeekNights']] = np.nan

    # Remove rows with NaN values
    df_cleaned = df.dropna()

    return df_cleaned


def feature_engineer( data: pd.DataFrame) -> pd.DataFrame:

    df = data.copy()

    #Top 10 Countries
    # Define the list of countries to consider as individual values
    top10_countries = ['PRT', 'FRA', 'DEU', 'GBR', 'ESP', 'ITA', 'BEL', 'BRA', 'USA', 'NLD']

    # Replace countries not in the top 10 with 'Other'
    df.loc[~df['Country'].isin(top10_countries), 'Country'] = 'Other'
    # Calculate the count of each country and sort by count in descending order
    country_counts = df['Country'].value_counts().sort_values(ascending=False)

    # Define the order of the countries with 'Other' as the last category
    country_order = list(country_counts.index)
    country_order.remove('Other')
    country_order.append('Other')

    #ArrivalDate
    df['ArrivalDateYear'] = df['ArrivalDateYear'].astype(str)
    df['ArrivalDateMonth'] = df['ArrivalDateMonth'].astype(str)
    df['ArrivalDateDayOfMonth'] = df['ArrivalDateDayOfMonth'].astype(str)

    df['ArrivalDate'] = df['ArrivalDateYear'] + '-' + df['ArrivalDateMonth'] + '-' + df['ArrivalDateDayOfMonth']

    df['ArrivalDate'] = pd.to_datetime(df['ArrivalDate']).astype('datetime64[ns]')

    df['ArrivalDateYear'] = df['ArrivalDateYear'].astype(int)

    months_mapping = {
    'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
    'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
}
    df['ArrivalDateMonth'] = df['ArrivalDateMonth'].map(months_mapping).astype(int)

    df['ArrivalDateDayOfMonth'] = df['ArrivalDateDayOfMonth'].astype(int)
    
    #IsPortugueseHoliday
    # Convert 'ArrivalDate' column to datetime
    df['ArrivalDate'] = pd.to_datetime(df['ArrivalDate'])

    #TotalGuests
    df['TotalGuests'] = (df['Adults'] + df['Children'] + df['Babies']).astype('int64')

    # WithBabies
    df['WithBabies'] = df['Babies'].apply(lambda x: 1 if x > 0 else 0)

    # WithChildren
    df['Children'] = df['Children'].astype('int64')
    df['WithChildren'] = df['Children'].apply(lambda x: 1 if x > 0 else 0)

    # AdultsOnly
    df['AdultsOnly'] = ((df['Adults'] >  0 ) & (df['WithBabies'] == 0) & (df['WithChildren'] == 0)).astype('int64')

    # IsFamily
    df['IsFamily'] = ((df['Adults'] >  0 ) & ((df['Children'] > 1) | (df['Babies'] > 1)) ).astype('int64')

    # IsTwo
    df['IsTwo'] = ((df['Adults'] == 2) & (df['Children'] == 0) & (df['Babies'] == 0)).astype('int64')

    # IsGroup
    df['IsGroup'] = (df['Adults'] >= 3).astype('int64')

    # TotalLengthOfStay 
    df['TotalLengthOfStay'] = df['StaysInWeekendNights'] + df['StaysInWeekNights'].astype('int64')

    #Season
    df['Season'] = df['ArrivalDateMonth'].apply(lambda x: 'Winter' if x in [1, 2, 12] else 'Spring' if x in [3, 4, 5] else 'Summer' if x in [6, 7, 8] else 'Autumn')

    #Semester
    df['Semester'] = df['ArrivalDateMonth'].apply(lambda x: 1 if x <= 6 else 2)

    #Quarter
    df['Quarter'] = df['ArrivalDateMonth'].apply(lambda x: 1 if x in [1, 2, 3] else 2 if x in [4, 5, 6] else 3 if x in [7, 8, 9] else 4)

    #CancellationRate
    df['CancellationRate'] = df['PreviousCancellations'] / (df['PreviousCancellations'] + df['PreviousBookingsNotCanceled'])
    df['CancellationRate'].fillna(0, inplace=True)

    #IsNewBooking
    df['IsNewBooking'] = (df['PreviousCancellations'] + df['PreviousBookingsNotCanceled']) == 0
    df['IsNewBooking'] = df['IsNewBooking'].astype(int)

    #HasSpecialRequest
    df['HasSpecialRequests'] = (df['TotalOfSpecialRequests'] > 0).astype(int)

    return df


def oh_encoder( data: pd.DataFrame) -> pd.DataFrame:

    df = data.copy()

    df.drop(['Company','AssignedRoomType','ReservationStatus',
          'ReservationStatusDate', 'BookingChanges', 'Country', 'ArrivalDate'],
            axis=1,
            inplace=True)

    cat_cols = ['Agent', 'ArrivalDateMonth', 'CustomerType',
            'DepositType', 'DistributionChannel',  'MarketSegment',
            'Meal', 'ReservedRoomType', 
             'Season', 'Semester', 'Quarter']
    
    bin_cols = ['IsCanceled', 'IsRepeatedGuest','WithChildren', 'WithBabies', 'AdultsOnly', 'IsFamily', 'IsTwo', 'IsGroup','IsNewBooking','HasSpecialRequests']
    
    num_cols = df.drop(cat_cols+bin_cols,axis=1).columns.to_list()

    #Encoding:
    # Create an instance of the OneHotEncoder
    encoder = OneHotEncoder(sparse=False)
    
    # Fit the encoder on the DataFrame and transform the data
    encoded_data = encoder.fit_transform(df[cat_cols])
    
    # Retrieve the column names based on the encoder's categories
    feature_names = encoder.get_feature_names_out(df[cat_cols].columns)
    feature_names = [re.sub(r'\s+', '', name).strip() for name in feature_names] # they were coming with a lot of spaces

    # Create a new DataFrame with the encoded data and column names
    encoded_df = pd.DataFrame(encoded_data, columns=feature_names)

    #Joining the dataframes
    df_num_cols = df[num_cols].reset_index(drop=True)
    encoded_df = encoded_df.join(df_num_cols)

    df_bin_cols = df[bin_cols].reset_index(drop=True)
    encoded_df = encoded_df.join(df_bin_cols)

     # Drop excessive features that make the dataset too sparse (present reasoning in nb) if they exist
    try:
      encoded_df.drop(['Agent_106', 'Agent_107', 'Agent_112', 'Agent_117', 'Agent_122',
                                   'Agent_141', 'Agent_144', 'Agent_148', 'Agent_150', 'Agent_158', 
                                   'Agent_162', 'Agent_167', 'Agent_170', 'Agent_180', 'Agent_193', 
                                   'Agent_196', 'Agent_197', 'Agent_211', 'Agent_213', 'Agent_216', 
                                   'Agent_227', 'Agent_232', 'Agent_235', 'Agent_236', 'Agent_24',
                                     'Agent_242', 'Agent_247', 'Agent_25', 'Agent_250', 'Agent_252',
                                       'Agent_256', 'Agent_265', 'Agent_267', 'Agent_269', 'Agent_270', 
                                       'Agent_276', 'Agent_278', 'Agent_280', 'Agent_283', 'Agent_285',
                                         'Agent_286', 'Agent_287', 'Agent_288', 'Agent_289', 'Agent_294', 
                                         'Agent_295', 'Agent_299', 'Agent_303', 'Agent_306', 'Agent_310', 
                                         'Agent_323', 'Agent_325', 'Agent_326', 'Agent_327', 'Agent_331', 
                                         'Agent_335', 'Agent_344', 'Agent_346', 'Agent_354', 'Agent_359',
                                           'Agent_36', 'Agent_364', 'Agent_370', 'Agent_371', 'Agent_375',
                                             'Agent_388', 'Agent_391', 'Agent_397', 'Agent_403', 'Agent_404',
                                               'Agent_408', 'Agent_41', 'Agent_416', 'Agent_427', 'Agent_436', 
                                               'Agent_441', 'Agent_444', 'Agent_449', 'Agent_453', 'Agent_455',
                                                 'Agent_459', 'Agent_461', 'Agent_464', 'Agent_467', 'Agent_47',
                                                   'Agent_474', 'Agent_475', 'Agent_476', 'Agent_480', 'Agent_495', 
                                                   'Agent_5', 'Agent_54', 'Agent_55', 'Agent_60', 'Agent_61', 
                                                   'Agent_63', 'Agent_64', 'Agent_70', 'Agent_72',
                                                     'Agent_73', 'Agent_74', 'Agent_78', 'Agent_90', 'Agent_92',
                                                       'Agent_93', 'Agent_95'], axis=1, inplace=True)
    except:
        pass

    log = logging.getLogger(__name__)
    log.info(f"The final dataframe has {len(encoded_df.columns)} columns.")

    return encoded_df

def ordinal_encoder( data: pd.DataFrame) -> pd.DataFrame:

    df = data.copy()

    df.drop(['Company','AssignedRoomType','ReservationStatus',
          'ReservationStatusDate', 'BookingChanges', 'Country', 'ArrivalDate'],
            axis=1,
            inplace=True)

    cat_cols = ['Agent', 'ArrivalDateMonth', 'CustomerType',
            'DepositType', 'DistributionChannel',  'MarketSegment',
            'Meal', 'ReservedRoomType', 
             'Season', 'Semester', 'Quarter']
    
    bin_cols = ['IsCanceled', 'IsRepeatedGuest','WithChildren', 'WithBabies', 'AdultsOnly', 'IsFamily', 'IsTwo', 'IsGroup','IsNewBooking','HasSpecialRequests']
    
    num_cols = df.drop(cat_cols+bin_cols,axis=1).columns.to_list()

    #Encoding:
    # Create an instance of the OneHotEncoder
    encoder = OrdinalEncoder()
    
    # Fit the encoder on the DataFrame and transform the data
    encoded_data = encoder.fit_transform(df[cat_cols])
    
    # Retrieve the column names based on the encoder's categories
    feature_names = encoder.get_feature_names_out(df[cat_cols].columns)
    
    # Create a new DataFrame with the encoded data and column names
    encoded_df = pd.DataFrame(encoded_data, columns=feature_names)

    #Joining the dataframes
    df_num_cols = df[num_cols].reset_index(drop=True)
    encoded_df = encoded_df.join(df_num_cols)

    df_bin_cols = df[bin_cols].reset_index(drop=True)
    encoded_df = encoded_df.join(df_bin_cols)

    # Drop excessive features that make the dataset too sparse (present reasoning in nb)
    encoded_df.drop(['Agent_106', 'Agent_107', 'Agent_112', 'Agent_117', 'Agent_122',
                                   'Agent_141', 'Agent_144', 'Agent_148', 'Agent_150', 'Agent_158', 
                                   'Agent_162', 'Agent_167', 'Agent_170', 'Agent_180', 'Agent_193', 
                                   'Agent_196', 'Agent_197', 'Agent_211', 'Agent_213', 'Agent_216', 
                                   'Agent_227', 'Agent_232', 'Agent_235', 'Agent_236', 'Agent_24',
                                     'Agent_242', 'Agent_247', 'Agent_25', 'Agent_250', 'Agent_252',
                                       'Agent_256', 'Agent_265', 'Agent_267', 'Agent_269', 'Agent_270', 
                                       'Agent_276', 'Agent_278', 'Agent_280', 'Agent_283', 'Agent_285',
                                         'Agent_286', 'Agent_287', 'Agent_288', 'Agent_289', 'Agent_294', 
                                         'Agent_295', 'Agent_299', 'Agent_303', 'Agent_306', 'Agent_310', 
                                         'Agent_323', 'Agent_325', 'Agent_326', 'Agent_327', 'Agent_331', 
                                         'Agent_335', 'Agent_344', 'Agent_346', 'Agent_354', 'Agent_359',
                                           'Agent_36', 'Agent_364', 'Agent_370', 'Agent_371', 'Agent_375',
                                             'Agent_388', 'Agent_391', 'Agent_397', 'Agent_403', 'Agent_404',
                                               'Agent_408', 'Agent_41', 'Agent_416', 'Agent_427', 'Agent_436', 
                                               'Agent_441', 'Agent_444', 'Agent_449', 'Agent_453', 'Agent_455',
                                                 'Agent_459', 'Agent_461', 'Agent_464', 'Agent_467', 'Agent_47',
                                                   'Agent_474', 'Agent_475', 'Agent_476', 'Agent_480', 'Agent_495', 
                                                   'Agent_5', 'Agent_54', 'Agent_55', 'Agent_60', 'Agent_61', 
                                                   'Agent_63', 'Agent_64', 'Agent_69', 'Agent_70', 'Agent_72',
                                                     'Agent_73', 'Agent_74', 'Agent_78', 'Agent_90', 'Agent_92',
                                                       'Agent_93', 'Agent_95', 'ReservedRoomType_P'], axis=1, inplace=True)

    log = logging.getLogger(__name__)
    log.info(f"The final dataframe has {len(encoded_df.columns)} columns.")

    return encoded_df