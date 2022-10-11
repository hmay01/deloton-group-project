import json
from os import getenv

import boto3


def production_sns_trigger(number_of_rows):
    topic_arn = 'arn:aws:sns:eu-west-2:605126261673:y_stories_stage'
    message = {"Number of rows": number_of_rows}
    sns_client = boto3.client(
        'sns', 
        aws_access_key_id=getenv('ACCESS_KEY_ID'), 
        aws_secret_access_key=getenv('SECRET_ACCESS_KEY'),
        region_name = 'eu-west-2')
    sns_client.publish(
        TopicArn=topic_arn, 
        Message=json.dumps({'default': json.dumps(message)}), 
        MessageStructure='json')
    print(f'SNS message sent to trigger production lambda.')


