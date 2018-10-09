import abc
from contextlib import contextmanager

import google.auth
import google.api_core
import google.oauth2
import google.cloud.exceptions
import google.cloud.bigquery

import dbt.clients.agate_helper
import dbt.exceptions
from dbt.adapters.base import BaseConnectionManager
from dbt.compat import abstractclassmethod
from dbt.logger import GLOBAL_LOGGER as logger


class BigQueryConnectionManager(BaseConnectionManager):
    TYPE = 'bigquery'

    SCOPE = ('https://www.googleapis.com/auth/bigquery',
             'https://www.googleapis.com/auth/cloud-platform',
             'https://www.googleapis.com/auth/drive')

    QUERY_TIMEOUT = 300

    @classmethod
    def handle_error(cls, error, message, sql):
        logger.debug(message.format(sql=sql))
        logger.debug(error)
        error_msg = "\n".join(
            [item['message'] for item in error.errors])

        raise dbt.exceptions.DatabaseException(error_msg)

    @contextmanager
    def exception_handler(self, sql, connection_name='master'):
        try:
            yield

        except google.cloud.exceptions.BadRequest as e:
            message = "Bad request while running:\n{sql}"
            self.handle_error(e, message, sql)

        except google.cloud.exceptions.Forbidden as e:
            message = "Access denied while running:\n{sql}"
            self.handle_error(e, message, sql)

        except Exception as e:
            logger.debug("Unhandled error while running:\n{}".format(sql))
            logger.debug(e)
            raise dbt.exceptions.RuntimeException(dbt.compat.to_string(e))

    def cancel_open(self):
        pass

    @classmethod
    def close(cls, connection):
        connection.state = 'closed'

        return connection

    def begin(self, name):
        pass

    def commit(self, connection):
        pass

    @classmethod
    def get_bigquery_credentials(cls, profile_credentials):
        method = profile_credentials.method
        creds = google.oauth2.service_account.Credentials

        if method == 'oauth':
            credentials, project_id = google.auth.default(scopes=cls.SCOPE)
            return credentials

        elif method == 'service-account':
            keyfile = profile_credentials.keyfile
            return creds.from_service_account_file(keyfile, scopes=cls.SCOPE)

        elif method == 'service-account-json':
            details = profile_credentials.keyfile_json
            return creds.from_service_account_info(details, scopes=cls.SCOPE)

        error = ('Invalid `method` in profile: "{}"'.format(method))
        raise dbt.exceptions.FailedToConnectException(error)

    @classmethod
    def get_bigquery_client(cls, profile_credentials):
        project_name = profile_credentials.project
        creds = cls.get_bigquery_credentials(profile_credentials)

        return google.cloud.bigquery.Client(project_name, creds)

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        try:
            handle = cls.get_bigquery_client(connection.credentials)

        except google.auth.exceptions.DefaultCredentialsError as e:
            logger.info("Please log into GCP to continue")
            dbt.clients.gcloud.setup_default_credentials()

            handle = cls.get_bigquery_client(connection.credentials)

        except Exception as e:
            raise
            logger.debug("Got an error when attempting to create a bigquery "
                         "client: '{}'".format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        connection.handle = handle
        connection.state = 'open'
        return connection

    @classmethod
    def get_timeout(cls, conn):
        credentials = conn['credentials']
        return credentials.get('timeout_seconds', cls.QUERY_TIMEOUT)

    @classmethod
    def get_table_from_response(cls, resp):
        column_names = [field.name for field in resp.schema]
        rows = [dict(row.items()) for row in resp]
        return dbt.clients.agate_helper.table_from_data(rows, column_names)

    def raw_execute(self, sql, model_name=None, fetch=False, **kwargs):
        conn = self.get(model_name)
        client = conn.handle

        logger.debug('On %s: %s', model_name, sql)

        job_config = google.cloud.bigquery.QueryJobConfig()
        job_config.use_legacy_sql = False
        query_job = client.query(sql, job_config)

        # this blocks until the query has completed
        with self.exception_handler(sql, conn.name):
            iterator = query_job.result()

        return query_job, iterator

    def execute(self, sql, model_name=None, fetch=None, **kwargs):
        _, iterator = self.raw_execute(sql, model_name, fetch, **kwargs)

        if fetch:
            res = self.get_table_from_response(iterator)
        else:
            res = dbt.clients.agate_helper.empty_table()

        # If we get here, the query succeeded
        status = 'OK'
        return status, res
