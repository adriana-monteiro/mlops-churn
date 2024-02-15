"""
This is a boilerplate pipeline
generated using Kedro 0.18.8
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import  unit_test_after_preprocessing


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            #Node gets engineered and encoded data to do unit tests on them
            node(
                func=unit_test_after_preprocessing,
                inputs=["hotel_engineered_data","hotel_encoded_data"],
                outputs=None,
                name="unit_data_test_prep",
            ),
        ]
    )


