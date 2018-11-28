from qa_python_utils.kafka.dispatcher import KafkaDispatcher
from mock import Mock


def test_instance():
    mock_producer = Mock()
    dispatcher = KafkaDispatcher(mock_producer)
    assert dispatcher.kafka_producer == mock_producer


def test_dispatch_message():
    mock_producer = Mock()
    dispatcher = KafkaDispatcher(mock_producer)

    dispatcher.dispatch_message('topic', 'event', 'payload', 'source', 10)

    mock_producer.send.assert_called_once_with(
        'topic', {
            'topic': 'topic',
            'eventName': 'event',
            'payload': 'payload',
            'source': 'source'
        })

    mock_producer.flush.assert_called_once_with(10)
