"""
2 ways to authenticate the Google Analytics Hook.
If you don't have an OAUTH token, you may authenticate by passing a
'client_secrets' object to the extras section of the relevant connection. This
object will expect the following fields and use them to generate an OAUTH token
on execution.In Airflow 1.9.0 this requires to use the web interface or cli to set connection extra's.
Recommended method is to supply a json keyfile with client secrets generated through UI. 
You can pass the keyfile path using keyfile variable. This hook overrides any client-secrets passed 
through UI with the keyfile.
If you have already obtained an OAUTH token, place it in the password field
of the relevant connection.
More details can be found here:
https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
"""

import time
import os

from airflow.hooks.base_hook import BaseHook
from airflow import configuration as conf
from apiclient.discovery import build
from apiclient.http import MediaInMemoryUpload
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.client import AccessTokenCredentials
from collections import namedtuple
from airflow.plugins_manager import AirflowPlugin



class GoogleAnalyticsHook(BaseHook):
    GAService = namedtuple('GAService', ['name', 'version', 'scopes'])
    # reporting service for reading and management service for writing . This hook allows only reporting(v4)
    _services = {
        'reporting': GAService(name='analyticsreporting',
                               version='v4',
                               scopes=['https://www.googleapis.com/auth/analytics.readonly']),
        'management': GAService(name='analytics',
                                version='v3',
                                scopes=['https://www.googleapis.com/auth/analytics'])
    }
    _key_folder = os.path.join(conf.get('core', 'airflow_home'), 'keys')

    def __init__(self, google_analytics_conn_id='google_analytics', key_file=None):
        self.google_analytics_conn_id = google_analytics_conn_id
        self.connection = self.get_connection(google_analytics_conn_id)
        if 'client_secrets' in self.connection.extra_dejson:
            self.client_secrets = self.connection.extra_dejson['client_secrets']
        if key_file:
            self.file_location = os.path.join(GoogleAnalyticsHook._key_folder, key_file)

    def get_service_object(self, name):
        service = GoogleAnalyticsHook._services[name]

        if self.connection.password:
            credentials = AccessTokenCredentials(self.connection.password,
                                                 'Airflow/1.0')
        elif hasattr(self, 'client_secrets'):
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(self.client_secrets,
                                                                           service.scopes)

        elif hasattr(self, 'file_location'):
            credentials = ServiceAccountCredentials.from_json_keyfile_name(self.file_location,
                                                                           service.scopes)
        else:
            raise ValueError('No valid credentials could be found')

        return build(service.name, service.version, credentials=credentials)


    def get_analytics_report(self,
                             view_id,
                             since,
                             until,
                             sampling_level,
                             dimensions,
                             metrics,
                             page_size,
                             include_empty_rows,
                             dimension_filter_clauses):

        analytics = self.get_service_object(name='reporting')

        reportRequest = {
            'viewId': view_id,
            'dateRanges': [{'startDate': since, 'endDate': until}],
            'samplingLevel': sampling_level or 'LARGE',
            'dimensions': dimensions,
            'metrics': metrics,
            'pageSize': page_size or 1000,
            'includeEmptyRows': include_empty_rows or False,
            'dimensionFilterClauses' : dimension_filter_clauses or None
        }

        response = (analytics
                    .reports()
                    .batchGet(body={'reportRequests': [reportRequest]})
                    .execute())

        if response.get('reports'):
            report = response['reports'][0]
            rows = report.get('data', {}).get('rows', [])

            while report.get('nextPageToken'):
                time.sleep(1)
                reportRequest.update({'pageToken': report['nextPageToken']})
                response = (analytics
                    .reports()
                    .batchGet(body={'reportRequests': [reportRequest]})
                    .execute())
                report = response['reports'][0]
                rows.extend(report.get('data', {}).get('rows', []))

            if report['data']:
                report['data']['rows'] = rows

            return report
        else:
            return {}

        

