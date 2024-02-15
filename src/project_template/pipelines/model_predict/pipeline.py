
"""
This is a boilerplate pipeline
generated using Kedro 0.18.8
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import  model_predict
from project_template.pipelines.data_preprocessing.nodes import clean_data, feature_engineer, oh_encoder

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            # Clean the data
            node(
                func=clean_data,
                inputs="hotel_raw_data",
                outputs="hotel_cleaned_data",
                name="clean",
            ),
            # Do our feature engineering
            node(
                func= feature_engineer,
                inputs="hotel_cleaned_data",
                outputs= "hotel_engineered_data",
                name="engineering",
            ),
            # Finally, one hot encode
            node(
                func=oh_encoder,
                inputs="hotel_engineered_data",
                outputs="hotel_encoded_data",
                name="encode",
            ),
            # Use test data and encoded data to predict. Use parameters.yml and the best columns to get all right things for our model
            node(
                func= model_predict,
                inputs=["test_model","X_test_data","y_test_data","hotel_encoded_data","parameters","best_columns"],
                outputs= "prediction",
                name="predict",
            ),
        ]
    )
