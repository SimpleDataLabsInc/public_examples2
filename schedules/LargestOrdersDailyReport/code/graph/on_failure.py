
from airflow.models import BaseOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule


def on_failure(config) -> BaseOperator:
    return EmailOperator(
        mime_charset='utf-8',
        to=config.email,
        subject='Largest Orders Daily Report Failed',
        html_content='This is urgent!',
        task_id='email_notification',
        trigger_rule='one_failed')

