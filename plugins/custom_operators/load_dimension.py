from airflow.utils.decorators import apply_defaults
from custom_operators.load_fact import LoadFactOperator


class LoadDimensionOperator(LoadFactOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 truncate_target=False,
                 table_name=None,
                 schema_name=None,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.truncate_target = truncate_target
        self.table_name = table_name
        self.schema_name = schema_name

    def execute(self, context):
        if self.truncate_target:
            truncate_query = 'TRUNCATE TABLE {}.{};'.format(self.schema_name,self.table_name)
            self.sql = '\n'.join([truncate_query, self.sql])

        super(LoadDimensionOperator, self).execute(context)
