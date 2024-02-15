"""
This is a boilerplate pipeline 'hyperparameter_search'
generated using Kedro 0.18.10
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

from sklearn.model_selection import RandomizedSearchCV
import time
from mlflow.tracking import MlflowClient



def hyperparameter_search(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.DataFrame, y_test: pd.DataFrame, parameters: Dict[str, Any],best_cols):

    # Use the best columns only
    X_train = X_train[best_cols]
    X_test = X_test[best_cols]

    # Reshape y to avoid warnings
    y_train = y_train.values.ravel()
    y_test = y_test.values.ravel() 

    # Defining the parmeters to search on
    param_grid = {
        'n_estimators': np.arange(50, 200, 10),
        'max_depth': [None] + list(np.arange(2, 30, 2)),
        'min_samples_split': np.arange(2, 20, 2),
        'min_samples_leaf': np.arange(1, 10, 1),
        'max_features': np.arange(2, 150, 5)
    }

    rf = RandomForestClassifier(random_state=parameters['random_state'])

    # Defining the random search with 3 folds and f1 score as the "control" score
    random_search = RandomizedSearchCV(rf, param_grid, n_iter=parameters['n_iter_rs'], 
                                       scoring='f1', n_jobs=-1, cv=parameters['cv_rs'], random_state=parameters['random_state'])
    
    mlflow.set_experiment("final_hyperparameter_search")
    mlflow.set_tag("mlflow.runName", "hyperparameter_search")

    # always inside the hypersearch experiment save runs
    with mlflow.start_run(nested=True) as run:

        # Fit the search and get the best model right away
        random_search.fit(X_train, y_train)
        
        best_model = random_search.best_estimator_
        
        test_score = best_model.score(X_test, y_test)

        saved_best_model = random_search.best_estimator_ #making sure we are saving here the best model
        
        mlflow.log_metric('best_validation_score', random_search.best_score_)
        mlflow.log_metric('best_test_score', test_score)
        mlflow.sklearn.log_model(best_model, "Best_model")
        mlflow.log_params(random_search.best_params_)

        mlflow.end_run()

    # log all others into mlflow. Note: we deleted 46 models in order to save space from final file.
    for i, params in enumerate(random_search.cv_results_['params']):
        model_name = f"Model_{i}"
        model = RandomForestClassifier(random_state=parameters['random_state']).set_params(**params)  # Create a new instance each model
            
        with mlflow.start_run(nested=True):
            model.fit(X_train, y_train)  
            train_score = model.score(X_train, y_train)  
            test_score = model.score(X_test, y_test) 
            
            mlflow.sklearn.log_model(model, model_name)
            mlflow.log_metric('train_score', train_score)  
            mlflow.log_metric('test_score', test_score) 
            mlflow.log_params(params) 
            
            mlflow.end_run()

    results_df = pd.DataFrame(random_search.cv_results_)

    # Get the run name from the outer run
    run_name = run.info.run_name

    log = logging.getLogger(__name__)
    log.info(f"Best model was the random forest with parameters: '{random_search.best_params_} with f1 score of {random_search.best_score_}")

    return saved_best_model, results_df