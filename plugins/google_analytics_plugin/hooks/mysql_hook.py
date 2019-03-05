# -*- coding: utf-8 -*-
# Provides a sqlalchemy mysql connection object 
# Use get_conn method to use within plugins.
# Specify charset utf-8 through UI
# Based of https://airflow.readthedocs.io/en/latest/_modules/airflow/hooks/mysql_hook.html
# 

import MySQLdb
import MySQLdb.cursors

from airflow.hooks.dbapi_hook import DbApiHook
from airflow.plugins_manager import AirflowPlugin
from sqlalchemy import create_engine


class MySqlHook(DbApiHook):

    conn_name_attr = 'mysql_conn_id'
    default_conn_name = 'mysql_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(MySqlHook, self).__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def set_autocommit(self, conn, autocommit):
        conn.autocommit(autocommit)

    def get_autocommit(self, conn):
        """
        :param conn: connection to get autocommit setting from.
        :type conn: connection object.
        :return: connection autocommit setting
        :rtype bool
        """
        return conn.get_autocommit()

    def get_conn(self):
        """
           Returns a sqlalchemy mysql connection object
        """
        conn = self.get_connection(self.mysql_conn_id)
        conn_config = {
            "user": conn.login,
            "passwd": conn.password or '',
            "host": conn.host or 'localhost',
            "db": self.schema or conn.schema or ''
        }

        if not conn.port:
            conn_config["port"] = 3306
        else:
            conn_config["port"] = int(conn.port)
        charset_str = "?charset=utf8"
        if conn.extra_dejson.get('charset', False):
            charset_str = ""
            conn_config["charset"] = conn.extra_dejson["charset"]
            if (conn_config["charset"]).lower() == 'utf8' or\
                    (conn_config["charset"]).lower() == 'utf-8':
                conn_config["use_unicode"] = True
        #if conn.extra_dejson.get('cursor', False):
        #    if (conn.extra_dejson["cursor"]).lower() == 'sscursor':
        #        conn_config["cursorclass"] = MySQLdb.cursors.SSCursor
        #    elif (conn.extra_dejson["cursor"]).lower() == 'dictcursor':
        #        conn_config["cursorclass"] = MySQLdb.cursors.DictCursor
        #    elif (conn.extra_dejson["cursor"]).lower() == 'ssdictcursor':
        #        conn_config["cursorclass"] = MySQLdb.cursors.SSDictCursor
        #local_infile = conn.extra_dejson.get('local_infile', False)
        #if conn.extra_dejson.get('ssl', False):
        #    conn_config['ssl'] = conn.extra_dejson['ssl']
        #if conn.extra_dejson.get('unix_socket'):
        #    conn_config['unix_socket'] = conn.extra_dejson['unix_socket']
        #if local_infile:
        #    conn_config["local_infile"] = 1

        sql_alchemy_uri = "mysql://" + str(conn.login) + ":" + str(conn.password) + "@" + str(conn.host) + \
                          "/" + str(conn.schema) + charset_str
        engine = create_engine(sql_alchemy_uri)
        conn = engine.connect()

        return conn

    def bulk_load(self, table, tmp_file):
        """
        Loads a tab-delimited file into a database table
        """
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("""
            LOAD DATA LOCAL INFILE '{tmp_file}'
            INTO TABLE {table}
            """.format(**locals()))
        conn.commit()

    def bulk_dump(self, table, tmp_file):
        """
        Dumps a database table into a tab-delimited file
        """
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT * INTO OUTFILE '{tmp_file}'
            FROM {table}
            """.format(**locals()))
        conn.commit()

    @staticmethod
    def _serialize_cell(cell, conn):
        """
        MySQLdb converts an argument to a literal
        when passing those separately to execute. Hence, this method does nothing.
        :param cell: The cell to insert into the table
        :type cell: object
        :param conn: The database connection
        :type conn: connection object
        :return: The same cell
        :rtype: object
        """

        return cell


