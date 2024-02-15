
"""
This is a boilerplate pipeline
generated using Kedro 0.18.8
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import  model_train


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            # Get the best model, train and test data, parameters and best columns to return a final test model.
            node(
                func= model_train,
                inputs=["best_model", "X_train_data","X_test_data","y_train_data","y_test_data","parameters","best_columns"],
                outputs= "test_model",
                name="train",
            ),
        ]
    )
