"""**DEPRECATED**
This operator requires custom O365 api https://github.com/Narcolapser/python-o365
Install steps: 1) git clone https://github.com/nihalbtq8/python-o365.git  \
                  or git clone https://github.com/Narcolapser/python-o365
               2) cd python-o365
               3) In airflow's virtualenv build and install:
                  python setup.py install
               4) pip install future
                """


from airflow.models import BaseOperator
import pandas as pd
import base64
import json
from datetime import datetime
from airflow.plugins_manager import AirflowPlugin
from google_analytics_plugin.hooks.mysql_hook import MySqlHook
from google_analytics_plugin.hooks.psql_hook import PSqlHook
from O365 import *

from airflow import configuration
from airflow.exceptions import AirflowConfigException


class OutlookOperator(BaseOperator):

    def __init__(self,
                 to,
                 subject=' ',
                 body=' ',
                 files=None,
                 html_flag=False,
                 input_task_id='',
                 input_dag_id='',
                 *args,
                 **kwargs):
                super().__init__(*args, **kwargs)

                self.to = to
                self.subject = subject
                self.body = body
                self.files = files
                self.html_flag = html_flag
                self.input_task_id = input_task_id
                self.input_dag_id = input_dag_id

    @staticmethod
    def attach_json(path, cid):
        with open(path, "rb") as att_file:
            encoded_string = base64.b64encode(att_file.read()).decode("utf-8")
        inline_json = """{
              "@odata.type": "#Microsoft.OutlookServices.FileAttachment",
              "Name": "%s",
              "IsInline": true,
              "ContentId": "%s",
              "ContentBytes": "%s"
               }""" % (path, cid, encoded_string)
        return json.loads(inline_json)

    def execute(self, context):
        try:
            OUTLOOK_USER = configuration.conf.get('outlook', 'OUTLOOK_USER')
            OUTLOOK_PASSWORD = configuration.conf.get('outlook', 'OUTLOOK_PASSWORD')
        except AirflowConfigException:
            raise AirflowConfigException("No user/password found for Outlook, so logging in with no authentication.")
        authentication = (OUTLOOK_USER, OUTLOOK_PASSWORD)

        m = Message(auth=authentication)
        m.setRecipients(self.to)
        m.setSubject(self.subject)
        if self.html_flag:
            m.setBodyHTML(self.body)
        else:
            m.setBody(self.body)
        if isinstance(self.files, (list,)):
            for file in self.files:
                att = Attachment(path=file)
                m.attachments.append(att)
        m.sendMessage()
        print("True")
        #self.log.info("Mail Sent.")

    send_mail = execute

