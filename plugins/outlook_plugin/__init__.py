from airflow.plugins_manager import AirflowPlugin
from outlook_plugin.operators.outlook_operator import OutlookOperator

class OutlookPlugin(AirflowPlugin):
    name = "outlook_plugin"
    hooks = []
    operators = [OutlookOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
