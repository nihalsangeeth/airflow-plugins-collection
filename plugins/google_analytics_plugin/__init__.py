from airflow.plugins_manager import AirflowPlugin
from google_analytics_plugin.hooks.mysql_hook import MySqlHook
from google_analytics_plugin.hooks.psql_hook import PSqlHook
from google_analytics_plugin.hooks.google_analytics_hook import GoogleAnalyticsHook
from google_analytics_plugin.operators.ga_mysql_operator import GoogleAnalyticsReportingToMySqlOperator
from google_analytics_plugin.operators.mysql_query_operator import MySqlQueryOperator
from google_analytics_plugin.operators.mysql_to_csv_operator import MySqlToCsvOperator 
from google_analytics_plugin.operators.mysql_to_mysql_operator import MySqlToMySqlOperator
from google_analytics_plugin.operators.mysql_to_psql_operator import MySqlToPSqlOperator

class GoogleAnalyticsPlugin(AirflowPlugin):
    name = "google_analytics_plugin"
    hooks = [GoogleAnalyticsHook, MySqlHook, PSqlHook]
    operators = [GoogleAnalyticsReportingToMySqlOperator, MySqlQueryOperator, MySqlToCsvOperator, \
                 MySqlToMySqlOperator, MySqlToPSqlOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
