
"""
This is a boilerplate pipeline
generated using Kedro 0.18.8
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import feature_selection


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            # Feature selection node receiving encoded and train splitted data and parameters from parameters.yml.
            # Returns the best columns to use in our model and a dataframe with the ordered features by importance.
            node(
                func=feature_selection,
                inputs=["hotel_encoded_data","X_train_data","y_train_data","parameters"],
                outputs=["best_columns","df_ordered_features"],
                name="model_feature_selection",
            ),
        ]
    )
