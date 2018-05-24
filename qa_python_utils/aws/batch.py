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
    def __check_running_jobs(self, job_name, job_queue, max_running_jobs):
        all_running_jobs = self.batch_client.list_jobs(
            jobQueue=job_queue,
            jobStatus='RUNNING',
            maxResults=1000
        )

        running_jobs = [job_summary['jobName'] for job_summary in all_running_jobs['jobSummaryList'] if
                        job_summary['jobName'] == job_name]

        _logger.info('m=__check_running_jobs, running_jobs={}'.format(len(running_jobs)))
        return len(running_jobs) >= max_running_jobs

    @logger
    def start_batch_job(self, job_name, job_queue, job_definition, command=None, vcpus=4, memory=4096,
                        max_running_jobs=1):
        if command is None or not isinstance(command, list):
            _logger.error('m=start_batch_job, command={}, msg=wrong params'.format(command))
            return

        check_result = self.__check_running_jobs(
            job_name=job_name,
            job_queue=job_queue,
            max_running_jobs=max_running_jobs
        )

        if check_result is True:
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
