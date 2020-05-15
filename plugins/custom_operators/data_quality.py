from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException
import sys

sys.path.append('/Path/To//sparkify_sql')
from sparkify_queries import SparkifyQueries


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        try:

            # Creating a connection to use the stored Credentials in the Copy command
            aws_hook = AwsHook("aws_credentials")
            credentials = aws_hook.get_credentials()

        except Exception as e:
            self.logger.error('Failed to establish AWS connection, {}'.format(e))
            raise AirflowException('Failed to establish AWS connection, {}'.format(e))

        try:
            # Executing the Copy command
            redshift_hook = PostgresHook(self.redshift_conn_id)

            for quality_check in SparkifyQueries.data_quality_checks:
                if quality_check.get('quality_rule') == 'COUNT_OF_RECORDS':
                    quality_check['result'] = redshift_hook.get_records(quality_check.get('quality_check_query'))[0][0]

                    if quality_check['result'] <= quality_check['min_count']:
                        raise AirflowException('Quality Rule {} for table {} has failed'.format(quality_check.get('quality_rule'),quality_check.get('table_name')))
                    else:
                        self.logger.info('Quality Rule {} for table {} has passed'.format(quality_check.get('quality_rule'),quality_check.get('table_name')))

        except Exception as e:
            self.logger.error('Failed execute quality checks, {}'.format(e))
            raise AirflowException('Failed execute quality checks, {}'.format(e))
