import sys

import boto3
from qa_python_utils.default_logger import logger, _logger

reload(sys)
sys.setdefaultencoding('utf8')


class BatchClient(object):
    """
    Client for accessing AWS Batch service through boto3
    """

    @logger
    def __init__(self):
        self.batch_client = boto3.client('batch')

    @logger
    def get_running_jobs_list(self, job_name, job_queue):
        all_running_jobs = self.batch_client.list_jobs(
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

        _logger.info(
            'm=__compare_running_instances, comparison_result={}'.format(len(running_jobs) >= comparison_value))
        return len(running_jobs) >= comparison_value

    @logger
    def is_job_running(self, job_name, job_queue):
        comparison_result = self.__compare_running_instances(
            job_name=job_name,
            job_queue=job_queue,
            comparison_value=1
        )

        _logger.info('m=is_job_running, is_running={}'.format(comparison_result))
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

        _logger.info('m=has_job_exceeded_max_running, running_jobs={}'.format(len(running_jobs)))
        return comparison_result

    @logger
    def start_batch_job(self, job_name, job_queue, job_definition, command=None, vcpus=4, memory=4096,
                        max_running_jobs=1):
        if command is None or not isinstance(command, list):
            _logger.error('m=start_batch_job, command={}, msg=wrong params'.format(command))
            return

        exceeded_max_running = self.has_job_exceeded_max_running(
            job_name=job_name,
            job_queue=job_queue,
            max_running_jobs=max_running_jobs
        )

        if exceeded_max_running is True:
            _logger.warn('m=start_batch_job, msg=exceeded max running jobs')
            return

        try:
            r = self.batch_client.submit_job(
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
            _logger.error(
                'm=start_batch_job, job_name={}, job_queue, job_definition={}, command={}, msg=error while submitting '
                'job'.format(
                    job_name, job_queue, job_definition, command))
            raise e
