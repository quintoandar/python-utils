import logging
import re
import sys
import time

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf8')

class AthenaClient(object):
    def __init__(self, staging_dir, *args, **kwargs):
        self.staging_dir = staging_dir
        self.athena_client = boto3.client('athena')
        self.s3_client = boto3.client('s3')
        self.bucket_folder_path = 'query_results'

    def execute_file_query(self, filename):
        _logger.info('m=execute_file_query, filename={}'.format(filename))

        with open(filename) as f:
            return self.execute_raw_query(sql=f.read())

    def execute_file_query_and_return_dataframe(self, filename):
        _logger.info('m=execute_file_query_and_return_dataframe, filename={}'.format(filename))

        query_execution_id = self.execute_file_query(filename)
        return self.get_dataframe_from_query_execution_id(query_execution_id)

    def execute_query_and_return_dataframe(self, sql):
        _logger.info('m=execute_query_and_return_dataframe, sql={}'.format(sql))

        query_execution_id = self.execute_raw_query(sql)
        return self.get_dataframe_from_query_execution_id(query_execution_id)

    def execute_raw_query(self, sql):
        _logger.info('m=execute_raw_query, sql={}'.format(sql))

        s3_staging_dir = 's3://{}/{}/'.format(self.staging_dir, self.bucket_folder_path)
        response = self.athena_client.start_query_execution(
            QueryString=sql,
            ResultConfiguration={
                'OutputLocation': s3_staging_dir
            }
        )

        return response['QueryExecutionId']

    def get_dataframe_from_query_execution_id(self, query_execution_id, check_sleep_time=2):
        _logger.info(
            'm=get_dataframe_from_query_execution_id, query_execution_id={0}, msg=getting object \'{0}\' from s3 folder path \'{1}/{2}\''.format(
                query_execution_id,
                self.staging_dir,
                self.bucket_folder_path))

        self.__wait_for_query_results(query_execution_id, check_sleep_time)
        return pd.read_csv('s3://{}/{}/{}.csv'.format(self.staging_dir, self.bucket_folder_path, query_execution_id))

    def __wait_for_query_results(self, query_execution_id, check_sleep_time=2):
        _logger.info('m=__wait_for_query_results, query_execution_id={}, check_sleep_time={}'.format(query_execution_id,
                                                                                                     check_sleep_time))

        query_execution_status = self.__get_query_execution_status(query_execution_id)
        start_time = time.time()
        while query_execution_status in ('QUEUED', 'RUNNING'):
            time.sleep(check_sleep_time)

            if time.time() - start_time > 7200:
                self.athena_client.stop_query_execution(QueryExecutionId=query_execution_id)
                raise Exception('msg=query execution timed out')

            query_execution_status = self.__get_query_execution_status(query_execution_id)

        if query_execution_status in ('FAILED', 'CANCELLED'):
            raise Exception('msg=query execution status {}, time_elapsed={}'.format(query_execution_status,
                                                                                    time.time() - start_time))

    def __get_query_execution_status(self, query_execution_id):
        query_execution = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        return query_execution['QueryExecution']['Status']['State']

    def execute_query_and_wait_for_results(self, sql):
        _logger.info('m=execute_query_and_wait_for_results, sql={}'.format(sql))

        query_execution_id = self.execute_raw_query(sql)
        self.__wait_for_query_results(query_execution_id)

    def create_parquet(self, key, query, raw_columns, clean_columns=None):
        _logger.info('m=create_parquet, key={}, query={}, msg=querying on athena...'.format(key, query))

        data = self.execute_query_and_return_dataframe(query)
        data = data.astype(object).where(pd.notnull(data), None)

        new_data = pd.DataFrame()
        if raw_columns is not None:
            for index, col_key in enumerate(raw_columns):
                col, _type = col_key, raw_columns[col_key]
                for row in xrange(data[col].shape[0]):
                    data.loc[row, col] = _type(data.loc[row, col]) if data.loc[row, col] else None

                    if clean_columns:
                        list_clean_columns = clean_columns.keys()
                        new_value = data.loc[row, col]

                        if type(clean_columns[list_clean_columns[index]]) == list:
                            regex_from = clean_columns[list_clean_columns[index]][1]
                            regex_to = clean_columns[list_clean_columns[index]][2]
                            new_value = re.sub(regex_from, regex_to, _type(data.loc[row, col]))

                        new_data.loc[row, list_clean_columns[index]] = new_value if new_value else None

        _logger.info('m=create_parquet, msg=creating parquet file')
        table = pa.Table.from_pandas(df=new_data if clean_columns else data)
        with pa.BufferOutputStream() as file_handler:
            pq.write_table(table, file_handler)

        _logger.info('m=create_parquet, msg=saving to s3')
        self.s3_client.put_object(Bucket=self.staging_dir, Key=key, Body=file_handler.get_result().to_pybytes())

        _logger.info('m=create_parquet, msg={} ready!'.format(key))

    def create_athena_table_with_json_serde(self, database, table_name, schema, location, partitions=None,
                                            serde_options=None, drop_if_exists=True):
        self._create_athena_table(database=database, table_name=table_name, schema=schema, location=location,
                                  partitions=partitions, serde='org.openx.data.jsonserde.JsonSerDe',
                                  serde_options=serde_options, drop_if_exists=drop_if_exists)

    def __create_athena_table(self, database, table_name, schema, location, serde, partitions=None,
                              serde_options=None, drop_if_exists=True):
        if drop_if_exists:
            self.execute_query_and_wait_for_results("""DROP TABLE IF EXISTS {}.{}""".format(database, table_name))

        query = """CREATE EXTERNAL TABLE IF NOT EXISTS {}.{} ({}) """.format(database, table_name, schema)

        if partitions is not None:
            query += """PARTITIONED BY ({}) """.format(partitions)

        query += """ROW FORMAT SERDE {} """.format(serde)

        if serde_options is not None:
            query += """WITH SERDEPROPERTIES ({}) """.format(serde_options)

        query += """LOCATION '{}'""".format(location)

        _logger.info('m=create_parquet, msg=Trying to create {}.{}...'.format(database, table_name))

        self.execute_query_and_wait_for_results(query)

        _logger.info(
            'm=create_parquet, msg=Table created! If it has partitions and you need them right now, run msck_repair_table function.')

    def msck_repair_table(self, database, table_name):
        self.execute_query_and_wait_for_results("""MSCK REPAIR TABLE {}.{}""".format(database, table_name))

    def update_partitions(self, table, location):
        # An alternative approach would be to simply use an
        # "msck repair table fastly" statement but this is very slow at Athena.
        # we will pay to list all S3 keys, so try to be efficient with the location choice
        #
        #
        # if not location.endswith('/'):
        #     location += '/'
        #
        # bucket_path = 's3://{}/'.format(self.amplitude_bucket)
        # prefix = location[len(bucket_path):]
        #
        # all_keys = boto3.resource('s3').Bucket(self.amplitude_bucket).objects.filter(Prefix=prefix)
        #
        # objects = set([k.key for k in all_keys if k.key.endswith('.gz')])
        #
        # partitions = set()
        # for o in objects:
        #     partitions.add(o[:len(o) - o[::-1].find('/')])
        #
        # for p in list(partitions):
        #     pp = re.split('/|=', p)
        #
        #     sql = """ALTER TABLE {} ADD IF NOT EXISTS""".format(table)
        #     sql += """PARTITION (app = {}, event_type = '{}', server_upload_date = date '{}')""". \
        #         format(pp[1], pp[3], pp[5])
        #     sql += """LOCATION '{}'""".format(bucket_path + p)
        #
        #     _logger.info('m=update_partitions, msg=Adding new partition at {}'.format(bucket_path + p))
        #     self._execute_query(sql)

        # TODO Need to figure out how to implement this one to be generic at location and partitions!
        raise NotImplementedError
