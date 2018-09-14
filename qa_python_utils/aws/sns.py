import json
import logging
import os
import boto3
import datetime
import arrow


logger = logging.getLogger(__name__)


default_kwargs = {
    'region_name': os.getenv('AWS_REGION', 'us-east-1')}


class JsonDecoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.date):
            return arrow.get(obj).format('YYYY-MM-DD')
        return json.JSONEncoder.default(self, obj)


class SNSClient:
    def __init__(self, topic_arn, kwargs={}):
        logger.info('initializing sns client for %s' % topic_arn)
        self.topic_arn = topic_arn
        default_kwargs.update(kwargs)
        try:
            self.client = boto3.client('sns', **default_kwargs)
        except:
            logger.exception('failed to init sqs client for %s' % topic_arn)

    def send(self, topic, payload):
        try:
            json_message = json.dumps({'topic': topic,
                                       'payload': payload},
                                      ensure_ascii=False,
                                      cls=JsonDecoder)
            response = self.client.publish(TopicArn=self.topic_arn,
                                           Message=json_message)
            if not(response['MessageId'] and
                   response['ResponseMetadata']['HTTPStatusCode'] == 200):
                raise Exception('failed to send message to sns.'
                                'message: %s' % json_message)
        except TypeError:
            logger.exception('Error processing message: %s', payload)
            raise
