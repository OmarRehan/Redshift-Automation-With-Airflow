from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin
from custom_operators import copy_to_redshift_operator, load_fact, load_dimension, data_quality


class CustomPlugin(AirflowPlugin):
    name = "custom_plugin"
    operators = [
        copy_to_redshift_operator.CopyToRedshiftOperator,
        load_fact.LoadFactOperator,
        load_dimension.LoadDimensionOperator,
        data_quality.DataQualityOperator
    ]
