from airflow.plugins_manager import AirflowPlugin
from mysql_gp_plugin.hooks.mysql_hook import MySqlHook
from mysql_gp_plugin.hooks.psql_hook import PSqlHook
from mysql_gp_plugin.operators.mysql_query_operator import MySqlQueryOperator
from mysql_gp_plugin.operators.mysql_to_csv_operator import MySqlToCsvOperator
from mysql_gp_plugin.operators.mysql_to_mysql_operator import MySqlToMySqlOperator
from mysql_gp_plugin.operators.mysql_to_psql_operator import MySqlToPSqlOperator
from mysql_gp_plugin.operators.embulk_operator import EmbulkOperator


class MySqlGPPlugin(AirflowPlugin):
    name = "mysql_gp_plugin"
    hooks = [MySqlHook, PSqlHook]
    operators = [MySqlQueryOperator, MySqlToCsvOperator, EmbulkOperator,\
                 MySqlToMySqlOperator, MySqlToPSqlOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
