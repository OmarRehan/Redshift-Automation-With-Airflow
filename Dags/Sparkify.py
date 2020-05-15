from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from custom_operators.copy_to_redshift_operator import CopyToRedshiftOperator
from custom_operators.load_fact import LoadFactOperator
from custom_operators.load_dimension import LoadDimensionOperator
from custom_operators.data_quality import DataQualityOperator
from airflow.operators.subdag_operator import SubDagOperator
import sys

sys.path.append('/Path/To//sparkify_sql')
from sparkify_queries import SparkifyQueries
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")

def func_dims_subdag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    with DAG(dag_id='%s.%s' % (parent_dag_name, child_dag_name), description='Load all Dims Data',
             start_date=start_date,
             schedule_interval=schedule_interval) as subdag:
        task_start_operator = DummyOperator(task_id='Begin_execution')

        task_load_song_dim = LoadDimensionOperator(
            task_id='Load_song_dim',
            redshift_conn_id='redshift',
            sql=SparkifyQueries.insert_song_table.format(**SparkifyQueries.dict_DDL_schemas,
                                                         **SparkifyQueries.dict_DDL_tables),
            truncate_target=False,
            schema_name=SparkifyQueries.dict_DDL_schemas.get('schema_sparkify_name'),
            table_name=SparkifyQueries.dict_DDL_tables.get('song_table')
        )

        task_load_user_dim = LoadDimensionOperator(
            task_id='Load_user_dim',
            redshift_conn_id='redshift',
            sql=SparkifyQueries.insert_user_table.format(**SparkifyQueries.dict_DDL_schemas,
                                                         **SparkifyQueries.dict_DDL_tables),
            truncate_target=False,
            schema_name=SparkifyQueries.dict_DDL_schemas.get('schema_sparkify_name'),
            table_name=SparkifyQueries.dict_DDL_tables.get('user_table')
        )

        task_load_time_dim = LoadDimensionOperator(
            task_id='Load_time_dim',
            redshift_conn_id='redshift',
            sql=SparkifyQueries.insert_time_table.format(**SparkifyQueries.dict_DDL_schemas,
                                                         **SparkifyQueries.dict_DDL_tables),
            truncate_target=False,
            schema_name=SparkifyQueries.dict_DDL_schemas.get('schema_sparkify_name'),
            table_name=SparkifyQueries.dict_DDL_tables.get('time_table')
        )

        task_load_artist_dim = LoadDimensionOperator(
            task_id='Load_artist_dim',
            redshift_conn_id='redshift',
            sql=SparkifyQueries.insert_artist_table.format(**SparkifyQueries.dict_DDL_schemas,
                                                           **SparkifyQueries.dict_DDL_tables),
            truncate_target=False,
            schema_name=SparkifyQueries.dict_DDL_schemas.get('schema_sparkify_name'),
            table_name=SparkifyQueries.dict_DDL_tables.get('artist_table')
        )

        end_operator = DummyOperator(task_id='Stop_execution')

        task_start_operator >> [task_load_song_dim, task_load_user_dim, task_load_time_dim, task_load_artist_dim] >> end_operator

    return subdag


default_args = {
    'owner': 'Sparkify',
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': True,
    'email_on_retry': False
}

with DAG('Sparkify',
         default_args=default_args,
         description='Load and transform data in Redshift with Airflow',
         schedule_interval='@daily',
         start_date=datetime(2018, 11, 3)
         ) as main_dag:
    task_start_operator = DummyOperator(task_id='Begin_execution')

    task_copy_logs_to_redshift = CopyToRedshiftOperator(
        task_id='copy_logs_to_redshift',
        redshift_conn_id='redshift',
        s3_path=f's3://udacity-dend/log_data/{{{{ds_nodash[:4]}}}}/{{{{ds_nodash[4:6]}}}}/{{{{ds}}}}-events.json',
        schema_name=SparkifyQueries.dict_DDL_schemas.get('schema_staging_name'),
        table_name=SparkifyQueries.dict_DDL_tables.get('stg_log_table'),
        file_path='s3://udacity-dend/log_json_path.json',
        provide_context=True
    )

    task_copy_songs_to_redshift = CopyToRedshiftOperator(
        task_id='copy_songs_to_redshift',
        redshift_conn_id='redshift',
        s3_path='s3://udacity-dend/song_data/',
        schema_name=SparkifyQueries.dict_DDL_schemas.get('schema_staging_name'),
        table_name=SparkifyQueries.dict_DDL_tables.get('stg_song_table'),
        provide_context=True
    )

    subdag_dims = SubDagOperator(subdag=func_dims_subdag('Sparkify', 'dims_dag', start_date=main_dag.start_date,
                                                         schedule_interval=main_dag.schedule_interval),
                                 task_id='dims_dag')

    task_load_songplay = LoadFactOperator(task_id='Load_songplay_fact',
                                          redshift_conn_id='redshift',
                                          sql=SparkifyQueries.insert_songplay_table.format(
                                              **SparkifyQueries.dict_DDL_schemas, **SparkifyQueries.dict_DDL_tables))

    task_quality_checks = DataQualityOperator(task_id='Quality_checks', redshift_conn_id='redshift')

    end_operator = DummyOperator(task_id='Stop_execution')

    task_start_operator >> [task_copy_logs_to_redshift,
                            task_copy_songs_to_redshift] >> subdag_dims >> task_load_songplay >> task_quality_checks >> end_operator
