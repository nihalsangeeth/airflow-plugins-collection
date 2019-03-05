#Returns a pandas dataframe with required query results.

from airflow.models import BaseOperator
import pandas as pd
from datetime import datetime
from airflow.plugins_manager import AirflowPlugin
from google_analytics_plugin.hooks.mysql_hook import MySqlHook


class MySqlQueryOperator(BaseOperator):

    def __init__(self,
                 mysql_conn_id,
                 database,
                 query,
                 *args,
                 **kwargs):
                super().__init__(*args, **kwargs)

                self.mysql_conn_id = mysql_conn_id
                self.database = database
                self.query = query

    def execute(self, context):

        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id, schema=self.database)
        conn = mysql_hook.get_conn()
        try:
            df_ = pd.read_sql(query, conn)
        except Exception as e:
            print("Error {0}".format(str(e)))
        return df_



