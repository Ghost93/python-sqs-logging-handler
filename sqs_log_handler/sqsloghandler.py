import logging
import logging.handlers

import boto3
from retrying import retry


class SQSHandler(logging.Handler):
    """ A Python logging handler which sends messages to Amazon SQS. """

    MAX_RETRY_ATTEMPTS = 3

    def __init__(self, queue, aws_key_id=None, secret_key=None, global_extra=None, region_name='eu-west-1'):
        """
        Sends log messages to SQS so downstream processors can consume
        (e.g. push the log messages to Splunk).
        :param queue: SQS queue name.
        :param aws_key_id: aws key id. Explicit credential parameters is
        not needed when running with EC2 role-based authentication.
        :param secret_key: secret key associated with the key id.
        """

        logging.Handler.__init__(self)
        client = boto3.resource('sqs',
                                aws_access_key_id=aws_key_id,
                                aws_secret_access_key=secret_key,
                                region_name=region_name)
        self.queue = client.get_queue_by_name(QueueName=queue)
        self._global_extra = global_extra

        # When self.emit is called, the emit function will call boto3 code,
        # which in-turn will generate logs, leading to infinitely nested
        # call to the log handler (when the log handler is attached to the
        # root logger). We use this flag to guard against nested calling.
        self._entrance_flag = False

    @retry(stop_max_attempt_number=MAX_RETRY_ATTEMPTS)
    def emit(self, record):
        """
        Emit log record by sending it over to AWS SQS queue.
        """
        if self._global_extra is not None:
            record.__dict__.update(self._global_extra)

        if not self._entrance_flag:
            msg = self.format(record)

            # When the handler is attached to root logger, the call on SQS
            # below could generate more logging, and trigger nested emit
            # calls. Use the flag to prevent stack overflow.
            self._entrance_flag = True
            try:
                self.queue.send_message(MessageBody=msg)
            finally:
                self._entrance_flag = False
