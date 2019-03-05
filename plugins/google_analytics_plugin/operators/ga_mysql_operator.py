# GA reporting v4 to MySQL ETL plugin
# xcom_push 'last_date' in upstream task. Provide an input_task_id to schedule this on day granularity. 
# Uses Pandas

from airflow.models import BaseOperator
import warnings
import pandas as pd
from datetime import datetime
from airflow.plugins_manager import AirflowPlugin
from google_analytics_plugin.hooks.mysql_hook import MySqlHook
from google_analytics_plugin.hooks.google_analytics_hook import GoogleAnalyticsHook


class GoogleAnalyticsReportingToMySqlOperator(BaseOperator):
    """
        Google Analytics Reporting To MySql Operator
        :param google_analytics_conn_id:                        The Google Analytics connection id. Override by key_file.
        :type google_analytics_conn_id:                         string
        :param view_id:                                         The view id for associated report.
        :type view_id:                                          string/array
        :param since:                                           The date up from which to pull GA data.
                                                                This can either be a string in the format
                                                                of '%Y-%m-%d %H:%M:%S' or '%Y-%m-%d'
                                                                but in either case it will be
                                                                passed to GA as '%Y-%m-%d'.
        :type since:                                            string
        :param until:                                           The date up to which to pull GA data.
                                                                This can either be a string in the format
                                                                of '%Y-%m-%d %H:%M:%S' or '%Y-%m-%d'
                                                                but in either case it will be
                                                                passed to GA as '%Y-%m-%d'.
        :type until:                                            string
        :param key_file:                                        GA credential file path.(Required)
        :type key_file:                                         string
        :param dimensions:                                      GA required report dimensions
        :type dimensions:                                       dict
        :param metrics:                                         GA required report metrics
        :type metrics:                                          dict       
        :param mysql_conn_id:                                   To mysql connection id.
        :type mysql_conn_id:                                    string
        :param database:                                        To mysql database.
        :type database:                                         string
        :param table:                                           To mysql table.
        :type table:                                            string
        """

    def __init__(self,
                 google_analytics_conn_id,
                 key_file,
                 view_id,
                 since,
                 until,
                 dimensions,
                 metrics,
                 mysql_conn_id,
                 database,
                 table,
                 input_task_id='',
                 page_size=10000,
                 include_empty_rows=True,
                 sampling_level=None,
                 if_exists='append',
                 column_map=None,
                 dtype_map=None,
                 dimension_filter_clauses=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.google_analytics_conn_id = google_analytics_conn_id
        self.view_id = view_id
        self.since = since
        self.until = until
        self.sampling_level = sampling_level
        self.dimensions = dimensions
        self.metrics = metrics
        self.page_size = page_size
        self.include_empty_rows = include_empty_rows
        self.key_file = key_file
        self.mysql_conn_id = mysql_conn_id
        self.database = database
        self.table = table
        self.if_exists = if_exists

        self.column_map = column_map
        self.dtype_map = dtype_map
        self.dimension_filter_clauses = dimension_filter_clauses
        self.input_task_id = input_task_id

        self.metric_map = {
            'METRIC_TYPE_UNSPECIFIED': 'varchar(255)',
            'CURRENCY': 'decimal(20,5)',
            'INTEGER': 'int(11)',
            'FLOAT': 'decimal(20,5)',
            'PERCENT': 'decimal(20,5)',
            'TIME': 'time'
        }

        if self.page_size > 10000:
            raise Exception('Please specify a page size equal to or lower than 10000.')

        if not isinstance(self.include_empty_rows, bool):
            raise Exception('Please specificy "include_empty_rows" as a boolean.')

    def execute(self, context):
        try:
            since_formatted = datetime.strptime(self.since, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
        except:
            since_formatted = str(self.since)
        if self.input_task_id:
            last_date = context['ti'].xcom_pull(task_ids=self.input_task_id, key='last_date')
            if last_date:
                since_formatted = str(last_date)

        try:
            until_formatted = datetime.strptime(self.until, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
        except:
            until_formatted = str(self.until)

        ga_conn = GoogleAnalyticsHook(self.google_analytics_conn_id, key_file=self.key_file)

        report = ga_conn.get_analytics_report(self.view_id,
                                              since_formatted,
                                              until_formatted,
                                              self.sampling_level,
                                              self.dimensions,
                                              self.metrics,
                                              self.page_size,
                                              self.include_empty_rows,
                                              self.dimension_filter_clauses)

        dimensionheaders = [header.replace('ga:', '') for header in report['columnHeader'].get('dimensions', [])]
        metricheaders = [entry.get('name').replace('ga:', '') for entry in
                         report['columnHeader'].get('metricHeader', {}).get('metricHeaderEntries', [])]

        dl = [row['dimensions'] + row['metrics'][0]['values'] for row in report.get('data', {}).get('rows', [])]
        df_ = pd.DataFrame(dl, columns=dimensionheaders + metricheaders)

        if self.dtype_map:
            for key, value in self.dtype_map.items():
                if key in df_.columns and value == 'date':
                    df_[key] = pd.to_datetime(df_[key], format='%Y%m%d', errors='coerce')
                if key in df_.columns and value == 'int':
                    df_[key] = pd.to_numeric(df_[key], errors='coerce')
                if key in df_.columns and value == 'float':
                    df_[key] = pd.to_numeric(df_[key], errors='coerce')

        if self.column_map:
            df_ = df_.rename(self.column_map, axis=1)

        self.log.info('Executing: %s', )
        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id, schema=self.database)
        conn = mysql_hook.get_conn()
        df_.to_sql(self.table, conn, if_exists=self.if_exists, index=False)

