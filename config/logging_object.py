# Sets up a logger object
import logging
import boto3
import io
# Configure aws s3 bucket to save logs and sns to send email when a portion of code fails


class ErrorLogger:

    def __init__(self, git_project_name, s3_bucket, s3_key_prefix, sns_topic_arn):
        # Create an in-memory strean stream for log messages so that we dont save lovally
        self.log_stream = io.StringIO()
        self.logger = self._setup_logger(git_project_name)
        self._sns_topic_arn = sns_topic_arn
        self.s3_bucket_name = s3_bucket
        self.s3_key_prefix = s3_key_prefix
        self.git_project_name = git_project_name
        self.sns = boto3.client('sns')

    def _setup_logger(self, git_project_name):
        logger = logging.getLogger(git_project_name)
        logger.setLevel(logging.WARNING)

        formatter = logging.Formatter('%(asctime)s - %(filename)s - %(funcName)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

        # Re move existing handlersn to avoid duplicatekjn log messages
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # Add stream handler to logger need to add stream handler and formatter
        stream_handler = logging.StreamHandler(self.log_stream)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        return logger

    def _save_to_s3(self):
        try:
            s3 = boto3.client('s3')
            self.log_stream.seek(0)

            # Upload the log_stream to S3
            s3_key_prefix_new = f"{self.s3_key_prefix}{self.git_project_name}"
            s3.upload_fileobj(io.BytesIO(self.log_stream.getvalue().encode('utf-8')),
                               self.s3_bucket_name,
                               s3_key_prefix_new)

            # Clear the log stream after successful upload
            self.log_stream.seek(0)
            # Moves the file pointer to the beginning of the in-memory stream and truncate clears the stream fole
            self.log_stream.truncate()

        except Exception as e:
            # Handle exceptions during S3 upload
            self.logger.error(f"Error uploading logs to S3: {repr(e)}")

    def upload_logs_to_s3(self):
        self._save_to_s3()

    def log_error(self, exception, message):
        try:
            self.logger.exception(message)
            self.log_stream.write(message + '\n')
            self._send_sns_notification(message)
        except Exception as e:
            self.logger.error(f"Error when trying to log error: {repr(e)}")

    def log_info(self, message):
        try:
            self.logger.info(message)
            self.log_stream.write(message + '\n')
        except Exception as e:
            self.logger.error(f"Error logging info: {repr(e)}")

    def log_debug(self, message):
        try:
            self.logger.debug(message)
            self.log_stream.write(message + '\n')
        except Exception as e:
            self.logger.error(f"Error logging debug: {repr(e)}")

    def log_warning(self, message):
        try:
            self.logger.warning(message)
            self.log_stream.write(message + '\n')
        except Exception as e:
            self.logger.error(f"Error logging warning: {repr(e)}")

    def _send_sns_notification(self, message):
        try:
            self.sns.publish(TopicArn=self._sns_topic_arn, Message=message)
        except Exception as e:
            self.logger.error(f"Error sending SNS notification: {repr(e)}")
