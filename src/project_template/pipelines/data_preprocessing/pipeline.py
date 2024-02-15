
"""
This is a boilerplate pipeline
generated using Kedro 0.18.8
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import  clean_data, feature_engineer, oh_encoder, ordinal_encoder

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=clean_data,
                inputs="hotel_raw_data",
                outputs="hotel_cleaned_data",
                name="clean",
            ),

            node(
                func= feature_engineer,
                inputs="hotel_cleaned_data",
                outputs= "hotel_engineered_data",
                name="engineering",
            ),

            node(
                func=oh_encoder,
                inputs="hotel_engineered_data",
                outputs="hotel_encoded_data",
                name="encode",
            ),

            # node(
            #     func=ordinal_encoder,
            #     inputs="hotel_engineered_data",
            #     outputs="hotel_encoded_data",
            #     name="encode",
            # ),

        ]
    )
