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

def model_train(best_model, X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.DataFrame, y_test: pd.DataFrame, parameters: Dict[str, Any], best_cols):
    
    # Choose if we want to do feature selection
    if  parameters["with_feature_selection"] == False:
        X_train_temp = X_train.copy()
        X_test_temp = X_test.copy()
        best_cols = list(X_train.columns)
    else:
        X_train_temp = X_train[best_cols].copy()
        X_test_temp = X_test[best_cols].copy()

    # Reshape for warning avoidance and avoid future errors or incorrect training and testing
    y_train = y_train.values.ravel()  # Reshape y (n_samples,)
    y_test = y_test.values.ravel()  # Reshape y (n_samples,)

    # Set an mlflow experiment which will save the trained model
    mlflow.set_experiment("final_model_trained")
    mlflow.set_tag("mlflow.runName", parameters["run_name"])

    #always inside the hypersearch experiment save runs
    with mlflow.start_run(nested=True) as run:

        # Get the best model from input and fit into all training set.
        model =  best_model
        model.fit(X_train_temp, y_train)

        # Do predictions and check accuracy and f1 score.
        preds = model.predict(X_test_temp)
        pred_labels = np.rint(preds)
        accuracy = sklearn.metrics.accuracy_score(y_test, pred_labels)
        f1 = sklearn.metrics.f1_score(y_test, pred_labels)
        
        mlflow.log_metric('accuracy', accuracy)
        mlflow.log_metric('f1_score', f1)
        mlflow.sklearn.log_model(model, "final_model")
        mlflow.log_params(model.get_params())

        mlflow.end_run()

    log = logging.getLogger(__name__)
    log.info(f"#Best columns: {len(best_cols)}")
    log.info("Model accuracy on test set: %0.2f%%", accuracy * 100)
    log.info("Model f1 score on test set: %0.2f%%", f1 * 100)


    return model