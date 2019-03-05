#ETL between 2 Mysql databases

from airflow.models import BaseOperator
import pandas as pd
from datetime import datetime
from airflow.plugins_manager import AirflowPlugin
from google_analytics_plugin.hooks.mysql_hook import MySqlHook


class MySqlToMySqlOperator(BaseOperator):

    def __init__(self,
                 mysql_conn_id_from,
                 database_from,
                 table_from,
                 mysql_conn_id_to,
                 database_to,
                 table_to,
                 if_exists_prd,         
                 *args,
                 **kwargs):
                super().__init__(*args, **kwargs)

                self.mysql_conn_id_from = mysql_conn_id_from
                self.database_from = database_from
                self.table_from = table_from
                self.mysql_conn_id_to = mysql_conn_id_to
                self.database_to = database_to
                self.table_to = table_to
                self.if_exists_prd = if_exists_prd  

    def execute(self, context):

        mysql_hook_from = MySqlHook(mysql_conn_id=self.mysql_conn_id_from, schema=self.database_from)
        conn_from = mysql_hook_from.get_conn()
        query = 'select * from ' + self.table_from
        df_ = pd.read_sql(query, conn_from)
        
        mysql_hook_to = MySqlHook(mysql_conn_id=self.mysql_conn_id_to, schema=self.database_to)
        conn_to = mysql_hook_to.get_conn()
        df_.to_sql(self.table_to, con=conn_to, if_exists=self.if_exists_prd, index=False)

        return True



