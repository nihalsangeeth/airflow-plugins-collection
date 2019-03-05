#ETL between Mysql to PostgreSQL databases

####################################################
import pandas as pd
from pandas.io.sql import SQLTable

''' This function modifies pandas builtin to_sql.__execute_insert\
 method for one insert statement for whole dataset instead of one\
 for each row. Change this for big datasets for exponential time \
 decrease in most cases.
'''
def _execute_insert(self, conn, keys, data_iter):
    print("Using monkey-patched _execute_insert")
    data = [dict((k, v) for k, v in zip(keys, row)) for row in data_iter]
    conn.execute(self.insert_statement().values(data))

SQLTable._execute_insert = _execute_insert


####################################################



from airflow.models import BaseOperator
from datetime import datetime
from airflow.plugins_manager import AirflowPlugin
from google_analytics_plugin.hooks.mysql_hook import MySqlHook
from google_analytics_plugin.hooks.psql_hook import PSqlHook


class MySqlToPSqlOperator(BaseOperator):

    def __init__(self,
				 mysql_conn_id_from,
				 database_from,
				 table_from,
				 psql_conn_id_to,
				 database_to,
				 table_to,
				 if_exists_prd,			
                 *args,
                 **kwargs):
                super().__init__(*args, **kwargs)

                self.mysql_conn_id_from = mysql_conn_id_from
                self.database_from = database_from
                self.table_from = table_from
                self.psql_conn_id_to = psql_conn_id_to
                self.database_to = database_to
                self.table_to = table_to
                self.if_exists_prd = if_exists_prd	

    def execute(self, context):

        mysql_hook_from = MySqlHook(mysql_conn_id=self.mysql_conn_id_from, schema=self.database_from)
        conn_from = mysql_hook_from.get_conn()
        query = 'select * from ' + self.table_from
        df_ = pd.read_sql(query, conn_from)

        psql_hook_to = PSqlHook(psql_conn_id=self.psql_conn_id_to, schema=self.database_to, )
        conn_to = psql_hook_to.get_conn()

        df_.to_sql(self.table_to, con=conn_to, if_exists=self.if_exists_prd, index=False)



