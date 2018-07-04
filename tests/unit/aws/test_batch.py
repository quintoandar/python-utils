from unittest import TestCase

import botocore.session
from botocore.stub import Stubber

from qa_python_utils.aws.batch import BatchClient


class AWSBatchTest(TestCase):
    def setUp(self):
        self.batch_session = botocore.session.get_session().create_client('batch', region_name='us-east-1')
        self.batch_stubber = Stubber(self.batch_session)

        with self.batch_stubber:
            self.batch_client = BatchClient()
        
    def tearDown(self):
        pass

    def test_get_job_info_by_id_none(self):
        # calls
        response = self.batch_client.get_job_info_by_id(job_id=None)

        # assertions
        self.assertIsNone(obj=response)

    def test_get_job_info_by_id_not_found(self):
        # mocks
        job_id = '-1'
        batch_response = {
            'jobs': []
        }
        self.batch_stubber.add_response(
            method='describe_jobs',
            service_response=batch_response
        )

        # calls
        with self.batch_stubber:
            self.batch_client._batch_client = self.batch_session
            response = self.batch_client.get_job_info_by_id(job_id=job_id)

        # assertions
        self.assertIsNone(obj=response)

    def test_get_job_info_by_id(self):
        # mocks
        job_id = '123'
        batch_response = {
            'jobs': [
                {
                    'jobId': job_id,
                    'status': 'status',
                    'jobName': 'job_name',
                    'jobQueue': 'job_queue',
                    'startedAt': 1234567890,
                    'jobDefinition': 'job_definition'
                }
            ]
        }
        self.batch_stubber.add_response(
            method='describe_jobs',
            service_response=batch_response
        )

        # calls
        with self.batch_stubber:
            self.batch_client._batch_client = self.batch_session
            response = self.batch_client.get_job_info_by_id(job_id=job_id)

        # assertions
        self.assertIsNotNone(obj=response)
        self.assertEqual(
            first=response,
            second=batch_response['jobs'][0]
        )

    def test_get_job_field_info_by_id_none(self):
        # mocks
        job_id = '123'
        batch_response = {
            'jobs': [
                {
                    'jobId': job_id,
                    'status': 'status',
                    'jobName': 'job_name',
                    'jobQueue': 'job_queue',
                    'startedAt': 1234567890,
                    'jobDefinition': 'job_definition'
                }
            ]
        }
        self.batch_stubber.add_response(
            method='describe_jobs',
            service_response=batch_response
        )

        # calls
        with self.batch_stubber:
            self.batch_client._batch_client = self.batch_session
            response = self.batch_client.get_job_field_info_by_id(
                field_name='field_name',
                job_id=job_id
            )

        # assertions
        self.assertIsNone(obj=response)

    def test_get_job_field_info_by_id(self):
        # mocks
        job_id = '123'
        batch_response = {
            'jobs': [
                {
                    'jobId': job_id,
                    'status': 'status',
                    'jobName': 'job_name',
                    'jobQueue': 'job_queue',
                    'startedAt': 1234567890,
                    'jobDefinition': 'job_definition'
                }
            ]
        }
        self.batch_stubber.add_response(
            method='describe_jobs',
            service_response=batch_response
        )

        # calls
        with self.batch_stubber:
            self.batch_client._batch_client = self.batch_session
            response = self.batch_client.get_job_field_info_by_id(
                field_name='jobId',
                job_id=job_id
            )

        # assertions
        self.assertIsNotNone(obj=response)
        self.assertEqual(
            first=response,
            second=job_id
        )

    def test_get_job_status_by_id(self):
        # mocks
        job_id = '123'
        status = 'status'
        batch_response = {
            'jobs': [
                {
                    'jobId': job_id,
                    'status': status,
                    'jobName': 'job_name',
                    'jobQueue': 'job_queue',
                    'startedAt': 1234567890,
                    'jobDefinition': 'job_definition'
                }
            ]
        }
        self.batch_stubber.add_response(
            method='describe_jobs',
            service_response=batch_response
        )

        # calls
        with self.batch_stubber:
            self.batch_client._batch_client = self.batch_session
            response = self.batch_client.get_job_field_info_by_id(
                field_name='status',
                job_id=job_id
            )

        # assertions
        self.assertIsNotNone(obj=response)
        self.assertEqual(
            first=response,
            second=status
        )

    def test_get_job_created_at_by_id(self):
        # mocks
        job_id = '123'
        created_at = 1234567890
        batch_response = {
            'jobs': [
                {
                    'jobId': job_id,
                    'status': 'status',
                    'jobName': 'job_name',
                    'jobQueue': 'job_queue',
                    'startedAt': 1234567890,
                    'jobDefinition': 'job_definition',
                    'createdAt': created_at
                }
            ]
        }
        self.batch_stubber.add_response(
            method='describe_jobs',
            service_response=batch_response
        )

        # calls
        with self.batch_stubber:
            self.batch_client._batch_client = self.batch_session
            response = self.batch_client.get_job_field_info_by_id(
                field_name='createdAt',
                job_id=job_id
            )

        # assertions
        self.assertIsNotNone(obj=response)
        self.assertEqual(
            first=response,
            second=created_at
        )

    def test_get_job_started_at_by_id(self):
        # mocks
        job_id = '123'
        started_at = 1234567890
        batch_response = {
            'jobs': [
                {
                    'jobId': job_id,
                    'status': 'status',
                    'jobName': 'job_name',
                    'jobQueue': 'job_queue',
                    'startedAt': started_at,
                    'jobDefinition': 'job_definition'
                }
            ]
        }
        self.batch_stubber.add_response(
            method='describe_jobs',
            service_response=batch_response
        )

        # calls
        with self.batch_stubber:
            self.batch_client._batch_client = self.batch_session
            response = self.batch_client.get_job_field_info_by_id(
                field_name='startedAt',
                job_id=job_id
            )

        # assertions
        self.assertIsNotNone(obj=response)
        self.assertEqual(
            first=response,
            second=started_at
        )

    def test_get_job_stopped_at_by_id(self):
        # mocks
        job_id = '123'
        stopped_at = 1234567890
        batch_response = {
            'jobs': [
                {
                    'jobId': job_id,
                    'status': 'status',
                    'jobName': 'job_name',
                    'jobQueue': 'job_queue',
                    'startedAt': 1234567890,
                    'jobDefinition': 'job_definition',
                    'stoppedAt': stopped_at
                }
            ]
        }
        self.batch_stubber.add_response(
            method='describe_jobs',
            service_response=batch_response
        )

        # calls
        with self.batch_stubber:
            self.batch_client._batch_client = self.batch_session
            response = self.batch_client.get_job_field_info_by_id(
                field_name='stoppedAt',
                job_id=job_id
            )

        # assertions
        self.assertIsNotNone(obj=response)
        self.assertEqual(
            first=response,
            second=stopped_at
        )
