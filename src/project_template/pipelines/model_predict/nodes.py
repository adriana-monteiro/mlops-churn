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


def model_predict(model, X_test: pd.DataFrame, y_test: pd.DataFrame, data: pd.DataFrame,  parameters: Dict[str, Any],best_cols):
    
    # Decide if we want feature selection or not
    if  parameters["with_feature_selection"] == False:
        X_test_temp = X_test.copy()
        data_predict = data.drop(columns=parameters["target_column"], axis=1)
    else:
        X_test_temp = X_test[best_cols].copy()
        data_predict = data[best_cols].copy()

    # predict with the model we input into this.
    preds = model.predict(X_test_temp)
    pred_labels = np.rint(preds)
    accuracy = sklearn.metrics.accuracy_score(y_test, pred_labels)
    f1 = sklearn.metrics.f1_score(y_test, pred_labels)
    
    log = logging.getLogger(__name__)
    log.info("Model accuracy on test set: %0.2f%%", accuracy * 100)
    log.info("Model f1 score on test set: %0.2f%%", f1 * 100)

    preds = model.predict(data_predict)
    data_predict["prediction"] = preds
    return data_predict