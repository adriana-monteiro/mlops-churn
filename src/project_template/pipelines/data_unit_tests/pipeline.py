"""
This is a boilerplate pipeline
generated using Kedro 0.18.8
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import  unit_test_before_preprocessing


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            # Node gets raw data and does the unit test.
            node(
                func=unit_test_before_preprocessing,
                inputs="hotel_raw_data",
                outputs="",
                name="unit_data_test",
            ),
        ]
    )


