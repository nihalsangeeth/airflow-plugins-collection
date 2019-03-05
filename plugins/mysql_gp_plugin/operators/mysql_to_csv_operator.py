#Outputs sql-query results to a csv flatfile.

from airflow.models import BaseOperator
import pandas as pd
from datetime import datetime
from airflow.plugins_manager import AirflowPlugin
from google_analytics_plugin.hooks.mysql_hook import MySqlHook


class MySqlToCsvOperator(BaseOperator):

    def __init__(self,
                 mysql_conn_id,
                 database,
                 query,
                 filename,
                 *args,
                 **kwargs):
                super().__init__(*args, **kwargs)

                self.mysql_conn_id = mysql_conn_id
                self.database = database
                self.query = query
                self.filename = filename

    def execute(self, context):

        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id, schema=self.database)
        conn = mysql_hook.get_conn()
        try:
            df_ = pd.read_sql(self.query, conn)
            df_.to_csv(self.filename)
        except Exception as e:
            print("Error {0}".format(str(e)))
        


