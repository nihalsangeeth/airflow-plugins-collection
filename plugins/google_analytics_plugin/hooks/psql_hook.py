# -*- coding: utf-8 -*-
# Provides a sqlalchemy postgresql connection object 
# Use get_conn method to use within plugins.
# Specify charset through UI. utf-8 by default.
# Based of https://github.com/apache/airflow/blob/master/airflow/hooks/postgres_hook.py
# 

import os
import psycopg2
import psycopg2.extensions
from contextlib import closing

from airflow.hooks.dbapi_hook import DbApiHook
from sqlalchemy import create_engine


class PSqlHook(DbApiHook):
    """
    Hook for Postgres using sqlalchemy
    """
    conn_name_attr = 'psql_conn_id'
    default_conn_name = 'psql_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(PSqlHook, self).__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def set_autocommit(self, conn, autocommit):
        """
        MySql connection sets autocommit in a different way.
        """
        conn.autocommit(autocommit)

    def get_autocommit(self, conn):
        """
        MySql connection gets autocommit in a different way.
        :param conn: connection to get autocommit setting from.
        :type conn: connection object.
        :return: connection autocommit setting
        :rtype bool
        """
        return conn.get_autocommit()

    def get_conn(self):
        """
        Returns a psql connection object
        """
        conn = self.get_connection(self.psql_conn_id)
        conn_config = {
            "user": conn.login,
            "passwd": conn.password or '',
            "host": conn.host or 'localhost',
            "db": self.schema or conn.schema or '',
            #"port": self.port or 6432,
            "port": '6432',
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

        sql_alchemy_uri = "postgresql+psycopg2://" + str(conn.login) + ":" + str(conn.password) + "@" + str(conn.host) + ":" + \
        str(conn_config["port"]) + "/" + str(conn.schema) + charset_str
        engine = create_engine(sql_alchemy_uri)
        conn = engine.connect()

        return conn


    def copy_expert(self, sql, filename, open=open):
        """
        Executes SQL using psycopg2 copy_expert method.
        Necessary to execute COPY command without access to a superuser.
        Note: if this method is called with a "COPY FROM" statement and
        the specified input file does not exist, it creates an empty
        file and no data is loaded, but the operation succeeds.
        So if users want to be aware when the input file does not exist,
        they have to check its existence by themselves.
        """
        if not os.path.isfile(filename):
            with open(filename, 'w'):
                pass

        with open(filename, 'r+') as f:
            with closing(self.get_conn()) as conn:
                with closing(conn.cursor()) as cur:
                    cur.copy_expert(sql, f)
                    f.truncate(f.tell())
                    conn.commit()

    def bulk_load(self, table, tmp_file):
        """
        Loads a tab-delimited file into a database table
        """
        self.copy_expert("COPY {table} FROM STDIN".format(table=table), tmp_file)

    def bulk_dump(self, table, tmp_file):
        """
        Dumps a database table into a tab-delimited file
        """
        self.copy_expert("COPY {table} TO STDOUT".format(table=table), tmp_file)

    @staticmethod
    def _serialize_cell(cell, conn):
        """
        Postgresql will adapt all arguments to the execute() method internally,
        hence we return cell without any conversion.
        See http://initd.org/psycopg/docs/advanced.html#adapting-new-types for
        more information.
        :param cell: The cell to insert into the table
        :type cell: object
        :param conn: The database connection
        :type conn: connection object
        :return: The cell
        :rtype: object
        """
        return cell
