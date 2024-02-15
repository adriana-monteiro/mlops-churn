
"""
This is a boilerplate pipeline
generated using Kedro 0.18.8
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import  data_drift
from project_template.pipelines.data_preprocessing.nodes import clean_data, feature_engineer, oh_encoder

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=clean_data,
                inputs="raw_data_predict",
                outputs="data_predict_cleaned",
                name="clean",
            ),

            node(
                func= feature_engineer,
                inputs="data_predict_cleaned",
                outputs= "data_predict_engineered",
                name="engineering",
            ),
            
            node(
                func= data_drift,
                inputs=["hotel_engineered_data","data_predict_engineered"],
                outputs= "drift_result",
                name="drift_analysis",
            ),
        ]
    )
