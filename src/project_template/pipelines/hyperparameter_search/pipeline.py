"""
This is a boilerplate pipeline 'hyperparameter_search'
generated using Kedro 0.18.10
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import  hyperparameter_search


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            # Node gets train and test data, parameters and the best columns to return the best model and a csv with all search results.
            node(
                func= hyperparameter_search,
                inputs=["X_train_data","X_test_data","y_train_data","y_test_data","parameters","best_columns"],
                outputs= ["best_model","search_results"],
                name="hyperparameter_search",
            ),
        ]
    )
