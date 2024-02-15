"""
This is a boilerplate pipeline
generated using Kedro 0.18.8
"""

import logging
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd


from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import OneHotEncoder , LabelEncoder
import shap 
import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score, classification_report
from sklearn.ensemble import RandomForestClassifier
import sklearn
import mlflow
import nannyml as nml

from evidently.report import Report
from evidently.metric_preset import DataDriftPreset

def data_drift(data_reference: pd.DataFrame, data_analysis: pd.DataFrame):

    cat_cols = ['Agent', 'ArrivalDateMonth', 'CustomerType',
            'DepositType', 'DistributionChannel',  'MarketSegment',
            'Meal', 'ReservedRoomType', 'Season', 'Semester', 'Quarter']
    
    bin_cols = ['IsCanceled', 'IsRepeatedGuest','WithChildren', 'WithBabies', 'AdultsOnly',
                 'IsFamily', 'IsTwo', 'IsGroup','IsNewBooking','HasSpecialRequests']
    
    num_cols = data_reference.drop(cat_cols+bin_cols,axis=1).columns.to_list()

    chunk_size = 2000

    # Let's initialize the object that will perform the Univariate Drift calculations
    multivariate_calculator = nml.DataReconstructionDriftCalculator(
    column_names= num_cols,
    chunk_size=chunk_size)

    multivariate_calculator.fit(data_reference)
    results = multivariate_calculator.calculate(data_analysis).to_df()

    #generate a report for some numeric features using KS test and evidely ai
    data_drift_report = Report(metrics=[
    DataDriftPreset(stattest='jensenshannon', stattest_threshold=0.05)])

    data_drift_report.run(current_data=data_analysis , reference_data=data_reference, column_mapping=None)
    data_drift_report.save_html("data/08_reporting/data_drift_report.html")
    return results