from qa_python_utils.kafka.dispatcher import KafkaDispatcher
from mock import Mock


def test_instance():
    mock_producer = Mock()
    dispatcher = KafkaDispatcher(mock_producer)
    assert dispatcher.kafka_producer == mock_producer
