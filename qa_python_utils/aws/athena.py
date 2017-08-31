import logging
import re
import sys
import time

import boto3
import fastparquet as fp
import pandas as pd
import s3fs

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf8')


class AthenaClient(object):
    def __init__(self, s3_bucket):
        self.s3_bucket = s3_bucket
        self.athena_client = boto3.client('athena')
        self.s3_client = boto3.client('s3')
        self.bucket_folder_path = 'query_results'

    def execute_file_query(self, filename, *params):
        _logger.info('m=execute_file_query, filename={}, params={}'.format(filename, params))

        with open(filename) as f:
            sql = f.read()
            return self.execute_raw_query(sql, *params)

    def execute_file_query_and_return_dataframe(self, filename, *params):
        _logger.info('m=execute_file_query_and_return_dataframe, filename={}, params={}'.format(filename, params))

        query_execution_id = self.execute_file_query(filename, *params)
        return self.get_dataframe_from_query_execution_id(query_execution_id)

    def execute_query_and_return_dataframe(self, sql, *params):
        _logger.info('m=execute_query_and_return_dataframe, sql={}, params={}'.format(sql, params))

        query_execution_id = self.execute_raw_query(sql, *params)
        return self.get_dataframe_from_query_execution_id(query_execution_id=query_execution_id, file_ext='csv')

    def execute_txt_query_and_return_dataframe(self, sql, *params):
        _logger.info('m=execute_txt_query_and_return_dataframe, sql={}, params={}'.format(sql, params))

        query_execution_id = self.execute_raw_query(sql, *params)
        return self.get_dataframe_from_query_execution_id(query_execution_id=query_execution_id, file_ext='txt')
        
    def execute_raw_query(self, sql, *params):
        _logger.info('m=execute_raw_query, sql={}, params={}'.format(sql, params))

        s3_staging_dir = 's3://{}/{}/'.format(self.s3_bucket, self.bucket_folder_path)
        if params:
            sql = sql.format(*params)
        response = self.athena_client.start_query_execution(
            QueryString=sql,
            ResultConfiguration={
                'OutputLocation': s3_staging_dir
            }
        )

        return response['QueryExecutionId']

    def get_dataframe_from_query_execution_id(self, query_execution_id, check_sleep_time=2, file_ext='csv'):
        _logger.info(
            'm=get_dataframe_from_query_execution_id, query_execution_id={0}, file_ext={3}, msg=getting object \'{0}\' from s3 folder path \'{1}/{2}\''.format(
                query_execution_id,
                self.s3_bucket,
                self.bucket_folder_path,
                file_ext))

        self.__wait_for_query_results(query_execution_id, check_sleep_time)
        return pd.read_csv('s3://{0}/{1}/{2}.{3}'.format(self.s3_bucket, self.bucket_folder_path, query_execution_id, file_ext),
                           keep_default_na=False, dtype=object, sep='\t' if file_ext == 'txt' else ',', header=-1 if file_ext == 'txt' else 'infer')

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
            raise Exception('status={}, time_elapsed={}, error_msg={}'.format(query_execution_status,
                                                                              time.time() - start_time,
                                                                              self.athena_client.get_query_execution(
                                                                                  QueryExecutionId=query_execution_id)[
                                                                                  'QueryExecution']['Status'][
                                                                                  'StateChangeReason']))

    def __get_query_execution_status(self, query_execution_id):
        query_execution = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        return query_execution['QueryExecution']['Status']['State']

    def execute_query_and_wait_for_results(self, sql, *params):
        _logger.info('m=execute_query_and_wait_for_results, sql={}, params={}'.format(sql, params))

        query_execution_id = self.execute_raw_query(sql, *params)
        self.__wait_for_query_results(query_execution_id)

        return query_execution_id

    def create_parquet_from_query(self, key, query, raw_columns=None, clean_columns=None):
        _logger.info('m=create_parquet_from_query, key={}, query={}, msg=querying on athena...'.format(key, query))

        df = self.execute_query_and_return_dataframe(query)
        self.create_parquet_from_df(key, df, raw_columns, clean_columns)

    def create_parquet_from_df(self, key, df, raw_columns=None, clean_columns=None):
        _logger.info('m=create_parquet_from_df')

        df = df.astype(object).where(pd.notnull(df), None)
        if raw_columns is None:
            new_df = df
        else:
            new_df = pd.DataFrame()
            for index, col_key in enumerate(raw_columns):
                col, _type = col_key, raw_columns[col_key]

                new_col = col if not clean_columns else clean_columns.keys()[index]
                new_df[new_col] = pd.Series([self.__format_entry(_type(entry), clean_columns,
                                                                 index) if entry is not None and entry != '' else None
                                             for entry in df.loc[:, col]])

        self.__save_df_file_into_s3_as_parquet(df=new_df, bucket=self.s3_bucket, file_path=key)

    def __format_entry(self, entry, clean_columns, column_index):
        if entry is None or clean_columns is None:
            return entry

        list_clean_columns = clean_columns.keys()
        new_type = clean_columns[list_clean_columns[column_index]]
        if type(new_type) != list:
            return new_type(entry)

        regex_from = clean_columns[list_clean_columns[column_index]][1]
        regex_to = clean_columns[list_clean_columns[column_index]][2]

        return new_type[0](re.sub(regex_from, regex_to, entry))

    def __save_df_file_into_s3_as_parquet(self, df, bucket, file_path):
        _logger.info('m=__save_df_file_into_s3_as_parquet')
        s3_fs = s3fs.S3FileSystem()
        fp.write('{}/{}'.format(bucket, file_path), df.where(df.notnull(), None), open_with=s3_fs.open)

        _logger.info('m=__save_df_file_into_s3_as_parquet, msg={} ready!'.format(file_path))

    def create_athena_table_with_json_serde(self, database, table_name, schema, location, partitions=None,
                                            serde_options=None, drop_if_exists=True):
        self.__create_athena_table(database=database, table_name=table_name, schema=schema, location=location,
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

        _logger.info('m=__create_athena_table, msg=Trying to create {}.{}...'.format(database, table_name))

        self.execute_query_and_wait_for_results(query)

        _logger.info(
            'm=__create_athena_table, msg=Table created! If it has partitions and you need them right now, run msck_repair_table function.')

    def msck_repair_table(self, database, table_name):
        self.execute_query_and_wait_for_results("""MSCK REPAIR TABLE {}.{}""".format(database, table_name))

    def upsert_single_partition(self, bucket_folder_path, database, table, partition_name, partition_value):
        _logger.info(
            'm=update_partitions, bucket_folder_path={}, database={}, table={}, partition_name={}, partition_value={}'.format(
                bucket_folder_path,
                database,
                table,
                partition_name,
                partition_value))

        drop_stmt = """ALTER TABLE {0}.{1} 
                        DROP IF EXISTS PARTITION ({2}='{3}')""".format(database, table, partition_name, partition_value)

        self.execute_query_and_wait_for_results(sql=drop_stmt)

        add_stmt = """ALTER TABLE {0}.{1} 
                       ADD IF NOT EXISTS PARTITION ({2}='{3}')
                       LOCATION 's3://{4}/{2}={3}'""".format(database, table, partition_name, partition_value,
                                                             bucket_folder_path)

        self.execute_query_and_wait_for_results(sql=add_stmt)

    def drop_single_partition(self, bucket_folder_path, database, table, partition_name, partition_value):
        _logger.info(
            'm=drop_single_partitions, bucket_folder_path={}, database={}, table={}, partition_name={}, partition_value={}'.format(
                bucket_folder_path,
                database,
                table,
                partition_name,
                partition_value))

        drop_stmt = """ALTER TABLE {0}.{1} 
                        DROP IF EXISTS PARTITION ({2}='{3}')""".format(database, table, partition_name, partition_value)
        self.execute_query_and_wait_for_results(sql=drop_stmt)
        
    def add_partition(self, database, table_name, partition):
        sql = 'ALTER TABLE {}.{} ADD IF NOT EXISTS PARTITION ({});'.format(
            database,
            table_name,
            partition
        )
        AthenaClient(self.s3_bucket).execute_raw_query(sql=sql)

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
