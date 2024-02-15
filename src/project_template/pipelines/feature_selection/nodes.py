import logging
from typing import Any, Dict, Tuple
import numpy as np
import pandas as pd
import json
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import OneHotEncoder , LabelEncoder
import shap 
import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score, classification_report
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import RFE
import sklearn



def feature_selection(data: pd.DataFrame, X_train: pd.DataFrame , y_train: pd.DataFrame,  parameters: Dict[str, Any]):
    """
    Args: 
        data[pd.DataFrame]: Useful for reference
        X_train[pd.DataFrame]: Train data from the split pipeline
        y_train[pd.DataFrame]: Target variable for training from the split pipeline

    Returns: 
        [pickle]: Best columns 
    """
    # If we do not want it to, feature selection will ignore all this and return all columns. Defined in parameters
    if parameters["feature_selection"] == "rfe":
        # Define our random forest
        model = RandomForestClassifier(n_estimators=parameters["n_estimators"], 
                                       max_depth=parameters["max_depth"], max_features=parameters["max_features"])
        
        # Reshape y (n_samples,) (this has to do with warning solving)
        y_train = y_train.values.ravel() 

        # Train the model and use RFE. Get the most important features.
        model.fit(X_train, y_train)
        rfe = RFE(model) 
        rfe = rfe.fit(X_train, y_train)
        f = rfe.get_support(1) #the most important features
        X_cols = X_train.columns[f].tolist()

        # Save feature importance for reporting
        feature_rankings = model.feature_importances_
        feature_names = X_train.columns
        ordered_features = [feature_names[idx] for idx in feature_rankings.argsort()]
        df_ordered = pd.DataFrame({'Feature': ordered_features, 'Importance': feature_rankings}).sort_values("Importance", ascending=False)

    log = logging.getLogger(__name__)
    log.info(f"Number of best columns is: {len(X_cols)}")
    return X_cols, df_ordered