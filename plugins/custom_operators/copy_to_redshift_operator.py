from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException


class CopyToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ['s3_path']

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 s3_path,
                 schema_name,
                 table_name,
                 file_path=None,
                 region='us-west-2',
                 *args, **kwargs):

        super(CopyToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.s3_path = s3_path
        self.schema_name = schema_name
        self.table_name = table_name
        self.file_path = file_path
        self.region = region

    def execute(self, context):

        try:

            # Creating a connection to use the stored Credentials in the Copy command
            aws_hook = AwsHook("aws_credentials")
            credentials = aws_hook.get_credentials()

        except Exception as e:
            self.logger.error('Failed to establish AWS connection, {}'.format(e))
            raise AirflowException('Failed to establish AWS connection, {}'.format(e))

        if self.file_path is None:
            copy_to_staging = f"""
            TRUNCATE TABLE {self.schema_name}.{self.table_name};
    
            COPY {self.schema_name}.{self.table_name}
            from '{self.s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            JSON 'auto' REGION '{self.region}';
            """
        else:
            copy_to_staging = f"""
            TRUNCATE TABLE {self.schema_name}.{self.table_name};

            COPY {self.schema_name}.{self.table_name}
            from '{self.s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            JSON '{self.file_path}' REGION '{self.region}';
            """

        try:
            # Executing the Copy command
            redshift_hook = PostgresHook(self.redshift_conn_id)
            redshift_hook.run(copy_to_staging)

        except Exception as e:
            self.logger.error('Failed to Copy the data to redshift, {}'.format(e))
            raise AirflowException('Failed to Copy the data to redshift, {}'.format(e))
