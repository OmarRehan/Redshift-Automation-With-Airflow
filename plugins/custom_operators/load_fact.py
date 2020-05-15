from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        try:
            # Executing the Copy command
            redshift_hook = PostgresHook(self.redshift_conn_id)
            cur = redshift_hook.get_cursor()
            cur.execute(self.sql)
            int_row_count = cur.rowcount
            cur.execute("END TRANSACTION;")
            self.logger.info(' {} Records have been Merged.'.format(int_row_count))

        except Exception as e:
            self.logger.error('Failed to load data, {}'.format(e))
            raise AirflowException('Failed to load data, {}'.format(e))
