import sys

import boto3

from qa_python_utils import QuintoAndarLogger

reload(sys)
sys.setdefaultencoding('utf8')

logger = QuintoAndarLogger('aws.batch')


class BatchClient(object):
    """
    Client for accessing AWS Batch service through boto3
    """

    @logger
    def __init__(self):
        self._batch_client = boto3.client('batch')

    @logger
    def get_running_jobs_list(self, job_name, job_queue):
        all_running_jobs = self._batch_client.list_jobs(
            jobQueue=job_queue,
            jobStatus='RUNNING',
            maxResults=1000
        )

        return [job_summary['jobName'] for job_summary in all_running_jobs['jobSummaryList'] if
                job_summary['jobName'] == job_name]

    @logger
    def __compare_running_instances(self, job_name, job_queue, comparison_value):
        running_jobs = self.get_running_jobs_list(
            job_name=job_name,
            job_queue=job_queue
        )

        logger.info(
            'm=__compare_running_instances, comparison_result={}'.format(len(running_jobs) >= comparison_value))
        return len(running_jobs) >= comparison_value

    @logger
    def is_job_running(self, job_name, job_queue):
        comparison_result = self.__compare_running_instances(
            job_name=job_name,
            job_queue=job_queue,
            comparison_value=1
        )

        logger.info('m=is_job_running, is_running={}'.format(comparison_result))
        return comparison_result

    @logger
    def has_job_exceeded_max_running(self, job_name, job_queue, max_running_jobs):
        comparison_result = self.__compare_running_instances(
            job_name=job_name,
            job_queue=job_queue,
            comparison_value=max_running_jobs
        )

        running_jobs = self.get_running_jobs_list(
            job_name=job_name,
            job_queue=job_queue
        )

        logger.info('m=has_job_exceeded_max_running, running_jobs={}'.format(len(running_jobs)))
        return comparison_result

    @logger
    def start_batch_job(self, job_name, job_queue, job_definition, command=None, vcpus=4, memory=4096,
                        max_running_jobs=1):
        if command is None or not isinstance(command, list):
            logger.error('m=start_batch_job, command={}, msg=wrong params'.format(command))
            return

        exceeded_max_running = self.has_job_exceeded_max_running(
            job_name=job_name,
            job_queue=job_queue,
            max_running_jobs=max_running_jobs
        )

        if exceeded_max_running is True:
            logger.warn('m=start_batch_job, msg=exceeded max running jobs')
            return

        try:
            r = self._batch_client.submit_job(
                jobName=job_name,
                jobQueue=job_queue,
                jobDefinition=job_definition,
                containerOverrides={
                    'vcpus': vcpus,
                    'memory': memory,
                    'command': command
                },
                retryStrategy={
                    'attempts': 1
                }
            )

            r.update({
                'status': 'SUBMITTED'
            })

            return r

        except Exception as e:
            logger.error(
                'm=start_batch_job, job_name={}, job_queue, job_definition={}, command={}, msg=error while submitting '
                'job'.format(
                    job_name, job_queue, job_definition, command))
            raise e

    @logger
    def get_job_info_by_id(self, job_id):
        if job_id is None:
            logger.error('m=get_job_info_by_id, job_id=None')
            return None

        job_description = self._batch_client.describe_jobs(
            jobs=[job_id]
        )

        if len(job_description['jobs']) == 0:
            logger.error('m=get_job_info_by_id, job_id={}, msg=job not found'.format(job_id))
            return None

        return job_description['jobs'][0]

    @logger
    def get_job_field_info_by_id(self, field_name, job_id):
        job_info = self.get_job_info_by_id(job_id=job_id)
        if field_name not in job_info:
            logger.warn('m=get_job_field_info_by_id, field_name={}, job_id={}'.format(field_name, job_id))
            return None

        return job_info[field_name]

    @logger
    def get_job_status_by_id(self, job_id):
        return self.get_job_field_info_by_id(
            field_name='status',
            job_id=job_id
        )

    @logger
    def get_job_created_at_by_id(self, job_id):
        return self.get_job_field_info_by_id(
            field_name='createdAt',
            job_id=job_id
        )

    @logger
    def get_job_started_at_by_id(self, job_id):
        return self.get_job_field_info_by_id(
            field_name='startedAt',
            job_id=job_id
        )

    @logger
    def get_job_stopped_at_by_id(self, job_id):
        return self.get_job_field_info_by_id(
            field_name='stoppedAt',
            job_id=job_id
        )
