#"""Project pipelines."""
#from typing import Dict

#from kedro.framework.project import find_pipelines
#from kedro.pipeline import Pipeline


#def register_pipelines() -> Dict[str, Pipeline]:
#    """Register the project's pipelines.

#    Returns:
#        A mapping from pipeline names to ``Pipeline`` objects.
#    """
#    pipelines = find_pipelines()
#    pipelines["__default__"] = sum(pipelines.values())
#    return pipelines


"""Project pipelines."""
from typing import Dict
from kedro.pipeline import Pipeline, pipeline

from project_template.pipelines import (
    data_preprocessing as preprocessing,
    data_split as split_data,
    model_train as train,
    feature_selection as best_features,
    model_predict as predict,
    data_drift as drift_test,
    hyperparameter_search as hyper_search,
    data_unit_tests as unit_test_before_preprocessing,
    data_unit_tests_after_prep as unit_test_after_preprocessing

)

def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    preprocessing_stage = preprocessing.create_pipeline()
    split_data_stage = split_data.create_pipeline()
    train_stage = train.create_pipeline()
    feature_selection_stage = best_features.create_pipeline()
    predict_stage = predict.create_pipeline()
    drift_test_stage = drift_test.create_pipeline()
    search_stage = hyper_search.create_pipeline()
    unit_test_bp_stage = unit_test_before_preprocessing.create_pipeline()
    unit_test_ap_stage = unit_test_after_preprocessing.create_pipeline()


    return {
        "unit_test_before_preprocessing": unit_test_bp_stage,
        "preprocessing": preprocessing_stage,
        "unit_test_after_preprocessing": unit_test_ap_stage,
        "split_data": split_data_stage,
        "train": train_stage,
        "feature_selection": feature_selection_stage,
        "predict": predict_stage,
        "drift_test" : drift_test_stage, 
        "optimize": preprocessing_stage + split_data_stage + feature_selection_stage + search_stage,
        "with_unit_tests": unit_test_bp_stage + preprocessing_stage + unit_test_ap_stage + split_data_stage + feature_selection_stage + train_stage,
        "__default__": preprocessing_stage + split_data_stage + feature_selection_stage + train_stage
    }