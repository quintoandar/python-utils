import re
import sys
import time
import json
import boto3
import botocore
import fastparquet as fp
import pandas as pd
import s3fs
from botocore.exceptions import ClientError

from qa_python_utils import QuintoAndarLogger

# while working with ipython notebooks, the stdout would be sent to the default tunnel (server)
# in order to work it around, the stdout needs to be stored and then reassigned after working with sys
stdout = sys.stdout
reload(sys)
sys.setdefaultencoding('utf8')
sys.stdout = stdout

logger = QuintoAndarLogger('aws.athena')


class AthenaClient(object):
    @logger(exclude=["aws_access_key_id", "aws_secret_access_key"])
    def __init__(self, s3_bucket=None, aws_access_key_id=None, aws_secret_access_key=None,
                 bucket_folder_path='query_results'):
        self.s3_bucket = s3_bucket

        if aws_access_key_id is not None and aws_secret_access_key is not None:
            self.athena_client = boto3.client('athena', aws_access_key_id=aws_access_key_id,
                                              aws_secret_access_key=aws_secret_access_key)
            self.s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                                              aws_secret_access_key=aws_secret_access_key)
        else:
            self.athena_client = boto3.client('athena')
            self.s3_resource = boto3.resource('s3')

        self.bucket_folder_path = bucket_folder_path

    @logger
    def execute_file_query(self, filename, query_params=None, s3_bucket=None, bucket_folder_path=None):
        with open(filename) as f:
            sql = f.read()
            return self.execute_raw_query(
                sql=sql.format(**query_params) if query_params else sql,
                s3_bucket=s3_bucket,
                bucket_folder_path=bucket_folder_path
            )

    @logger
    def execute_file_query_and_return_dataframe(self, filename, query_params=None, s3_bucket=None,
                                                bucket_folder_path=None):
        query_execution_id = self.execute_file_query(
            filename=filename,
            s3_bucket=s3_bucket,
            bucket_folder_path=bucket_folder_path,
            query_params=query_params
        )
        return self.get_dataframe_from_query_execution_id(query_execution_id)

    def execute_query_and_return_dataframe(self, sql, query_params=None, paginate=False, page_size=1000, s3_bucket=None,
                                           bucket_folder_path=None):
        logger.info(
            'm=execute_query_and_return_dataframe, sql={}, query_params={}, paginate={}, page_size={}, s3_bucket={}, '
            'bucket_folder_path={}'.format(
                sql, query_params, paginate, page_size, s3_bucket, bucket_folder_path))

        query_execution_id = self.execute_raw_query(
            sql=sql.format(**query_params) if query_params else sql,
            s3_bucket=s3_bucket,
            bucket_folder_path=bucket_folder_path
        )
        if paginate:
            return self.get_paginated_dataframe_from_query_execution_id(query_execution_id=query_execution_id,
                                                                        page_size=page_size)
        else:
            return self.get_dataframe_from_query_execution_id(query_execution_id=query_execution_id, file_ext='csv')

    def execute_txt_query_and_return_dataframe(self, sql, query_params=None, s3_bucket=None, bucket_folder_path=None):
        logger.info(
            'm=execute_txt_query_and_return_dataframe, sql={}, query_params={}, s3_bucket={}, bucket_folder_path={'
            '}'.format(
                sql, query_params, s3_bucket, bucket_folder_path))

        query_execution_id = self.execute_raw_query(
            sql=sql.format(**query_params) if query_params else sql,
            s3_bucket=s3_bucket,
            bucket_folder_path=bucket_folder_path
        )
        return self.get_dataframe_from_query_execution_id(query_execution_id=query_execution_id, file_ext='txt')

    @logger
    def execute_raw_query(self, sql, query_params=None, s3_bucket=None, bucket_folder_path=None):
        logger.info('m=execute_raw_query, sql={}, query_params=None, s3_bucket={}, bucket_folder_path={}'.format(
            sql,
            query_params,
            s3_bucket,
            bucket_folder_path))

        s3_bucket = s3_bucket or self.s3_bucket
        bucket_folder_path = bucket_folder_path or self.bucket_folder_path

        if s3_bucket is None or bucket_folder_path is None:
            raise RuntimeError(
                'm=execute_raw_query, s3_bucket={}, bucket_folder_path={}, msg=s3 path must be complete'.format(
                    s3_bucket, bucket_folder_path))

        count = 0
        while count < 3:
            try:
                response = self.athena_client.start_query_execution(
                    QueryString=sql.format(**query_params) if query_params else sql,
                    ResultConfiguration={
                        'OutputLocation': 's3://{}/{}/'.format(s3_bucket, bucket_folder_path)
                    }
                )
                return response['QueryExecutionId']
            except ClientError as e:
                logger.error(
                    'm={}.init, msg=query failed, it will be retried in 60 seconds. Error: {}.'.format(
                        'execute_raw_query', e.response['Error']['Message']))
                time.sleep(60)
                count += 1

        raise RuntimeError('m={}.init, msg=athena query failed three times.'.format('execute_raw_query'))

    @logger
    def __download_from_s3(self, key):
        try:
            self.s3_resource.Bucket(self.s3_bucket).download_file('{}/{}'.format(self.bucket_folder_path, key),
                                                                  '/tmp/{}'.format(key))
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.error("m=__download_from_s3, msg=The object does not exist.")
                return False
            else:
                raise
        return True

    @logger
    def get_dataframe_from_query_execution_id(self, query_execution_id, check_sleep_time=2, file_ext='csv'):
        self.wait_for_query_results(query_execution_id, check_sleep_time)
        key = '{0}.{1}'.format(query_execution_id, file_ext)
        self.__download_from_s3(key)
        return pd.read_csv('/tmp/{}'.format(key), keep_default_na=False, sep='\t' if file_ext == 'txt' else ',',
                           header=-1 if file_ext == 'txt' else 'infer')

    @logger
    def get_paginated_dataframe_from_query_execution_id(self, query_execution_id, check_sleep_time=2, page_size=1000):
        self.wait_for_query_results(query_execution_id, check_sleep_time)
        final_run = False
        first_run = True
        token = None
        columns = []
        while not final_run:
            data = []
            if token:
                result = self.athena_client.get_query_results(
                    QueryExecutionId=query_execution_id,
                    NextToken=token,
                    MaxResults=page_size
                )
            else:
                result = self.athena_client.get_query_results(
                    QueryExecutionId=query_execution_id,
                    MaxResults=page_size
                )
            if first_run:
                for value in result['ResultSet']['Rows'][0]['Data']:
                    columns.append(str(value.get('VarCharValue')))

                for rows in result['ResultSet']['Rows'][1:]:
                    row = []
                    for values in rows['Data']:
                        row.append(values.get('VarCharValue'))
                    data.append(row)
            else:
                for rows in result['ResultSet']['Rows']:
                    row = []
                    for values in rows['Data']:
                        row.append(values.get('VarCharValue'))
                    data.append(row)
            # update generator status
            first_run = False
            token = result.get('NextToken', None)
            final_run = False if token else True
            df = pd.DataFrame(data, columns=columns)
            yield df

    @logger
    def wait_for_query_results(self, query_execution_id, check_sleep_time=2):
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

    @logger
    def __get_query_execution_status(self, query_execution_id):
        query_execution = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        return query_execution['QueryExecution']['Status']['State']

    def execute_query_and_wait_for_results(self, sql, query_params=None, s3_bucket=None, bucket_folder_path=None):
        logger.info(
            'm=execute_query_and_wait_for_results, sql={}, query_params={}, s3_bucket={}, bucket_folder_path={}'.format(
                sql,
                query_params,
                s3_bucket,
                bucket_folder_path))

        query_execution_id = self.execute_raw_query(
            sql=sql.format(**query_params) if query_params else sql,
            s3_bucket=s3_bucket,
            bucket_folder_path=bucket_folder_path
        )

        self.wait_for_query_results(query_execution_id)
        return query_execution_id

    @logger
    def execute_file_query_and_wait_for_results(self, filename, query_params=None, s3_bucket=None,
                                                bucket_folder_path=None):
        query_execution_id = self.execute_file_query(
            filename=filename,
            s3_bucket=s3_bucket,
            bucket_folder_path=bucket_folder_path,
            query_params=query_params
        )
        self.wait_for_query_results(query_execution_id)
        return query_execution_id

    def create_parquet_from_query(self, key, query, query_params=None, raw_columns=None, clean_columns=None,
                                  s3_bucket=None,
                                  bucket_folder_path=None):
        logger.info('m=create_parquet_from_query, key={}, query={}'.format(key, query))

        df = self.execute_query_and_return_dataframe(
            sql=query.format(**query_params) if query_params else query,
            s3_bucket=s3_bucket,
            bucket_folder_path=bucket_folder_path
        )
        self.create_parquet_from_df(key, df, raw_columns, clean_columns)

    def create_parquet_from_df(self, key, df, raw_columns=None, clean_columns=None, s3_bucket=None):
        logger.info('m=create_parquet_from_df')

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

        self.__save_df_file_into_s3_as_parquet(df=new_df, bucket=s3_bucket or self.s3_bucket, file_path=key)

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
        logger.info('m=__save_df_file_into_s3_as_parquet')
        s3_fs = s3fs.S3FileSystem()
        fp.write('{}/{}'.format(bucket, file_path), df.where(df.notnull(), None),
                 open_with=s3_fs.open, row_group_offsets=500000)

        logger.info('m=__save_df_file_into_s3_as_parquet, msg={} ready!'.format(file_path))

    def create_athena_table_with_json_serde(self, database, table_name, schema, location, partitions=None,
                                            serde_options=None, drop_if_exists=True):
        self.__create_athena_table(database=database, table_name=table_name, schema=schema, location=location,
                                   partitions=partitions, serde='org.openx.data.jsonserde.JsonSerDe',
                                   serde_options=serde_options, drop_if_exists=drop_if_exists)

    def __create_athena_table(self, database, table_name, schema, location, serde, partitions=None, serde_options=None, drop_if_exists=True):
        if drop_if_exists:
            self.execute_query_and_wait_for_results("""DROP TABLE IF EXISTS {}.{}""".format(database, table_name))

        query = """CREATE EXTERNAL TABLE IF NOT EXISTS {}.{} ({}) """.format(database, table_name, schema)

        if partitions is not None:
            query += """PARTITIONED BY ({}) """.format(partitions)

        query += """ROW FORMAT SERDE {} """.format(serde)

        if serde_options is not None:
            query += """WITH SERDEPROPERTIES ({}) """.format(serde_options)

        query += """LOCATION '{}'""".format(location)

        logger.info('m=__create_athena_table, msg=Trying to create {}.{}...'.format(database, table_name))

        self.execute_query_and_wait_for_results(query)

        logger.info(
            'm=__create_athena_table, msg=Table created! If it has partitions and you need them right now, '
            'run msck_repair_table function.')

    def msck_repair_table(self, database, table_name):
        self.execute_query_and_wait_for_results("""MSCK REPAIR TABLE {}.{}""".format(database, table_name))

    def upsert_partitions(self, bucket_folder_path, database, table, partitions_list_dicts):

        logger.info('m=upsert_partitions, bucket_folder_path={}, database={}, table={}, partitions_list_dicts=({})'
                    .format(bucket_folder_path, database, table, json.dumps(partitions_list_dicts)))

        partition_list = []

        for partition in partitions_list_dicts:
            # create list of partitions
            partition_list.append("{0}='{1}'".format(partition['partition_name'], partition['partition_value']))
            # add path for each partition
            bucket_folder_path += '/{0}={1}'.format(partition['partition_name'], partition['partition_value'])

        drop_stmt = """ALTER TABLE {0}.{1}
                        DROP IF EXISTS PARTITION ({2})""".format(database, table, ','.join(partition_list))

        try:
            self.execute_query_and_wait_for_results(sql=drop_stmt)

            add_stmt = """ALTER TABLE {0}.{1}
                    ADD IF NOT EXISTS PARTITION ({2})
                    LOCATION 's3://{3}'""".format(database, table, ','.join(partition_list), bucket_folder_path)

            logger.info('m=upsert_partitions, statement: \n{}'.format(add_stmt))
            self.execute_query_and_wait_for_results(sql=add_stmt)

        except Exception:
            logger.warn('m=upsert_partitions, bucket_folder_path={}, database={}, table={}, partitions=({}), '
                        'msg=exception raised while deleting partition '.format(bucket_folder_path,
                                                                                database,
                                                                                table,
                                                                                ','.join(partition_list)))

    @logger
    def upsert_single_partition(self, bucket_folder_path, database, table, partition_name, partition_value):
        drop_stmt = """ALTER TABLE {0}.{1}
                        DROP IF EXISTS PARTITION ({2}='{3}')""".format(database, table, partition_name, partition_value)

        try:
            self.execute_query_and_wait_for_results(sql=drop_stmt)
        except Exception:
            logger.warn('m=upsert_single_partition, bucket_folder_path={}, database={}, table={}, partition_name={}, '
                        'partition_value={}, msg=exception raised while deleting partition'.format(bucket_folder_path,
                                                                                                   database, table,
                                                                                                   partition_name,
                                                                                                   partition_value))

        add_stmt = """ALTER TABLE {0}.{1}
                       ADD IF NOT EXISTS PARTITION ({2}='{3}')
                       LOCATION 's3://{4}/{2}={3}'""".format(database, table, partition_name, partition_value,
                                                             bucket_folder_path)

        self.execute_query_and_wait_for_results(sql=add_stmt)

    @logger
    def drop_single_partition(self, bucket_folder_path, database, table, partition_name, partition_value):
        drop_stmt = """ALTER TABLE {0}.{1}
                        DROP IF EXISTS PARTITION ({2}='{3}')""".format(database, table, partition_name, partition_value)
        self.execute_query_and_wait_for_results(sql=drop_stmt)

    @logger
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
        #     logger.info('m=update_partitions, msg=Adding new partition at {}'.format(bucket_path + p))
        #     self._execute_query(sql)

        # TODO Need to figure out how to implement this one to be generic at location and partitions!
        raise NotImplementedError
