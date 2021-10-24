import boto3
from botocore.exceptions import ClientError
import json
from urllib.parse import unquote_plus
import logging
import ast


print('Loading budget-stream function')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')


def lambda_handler(event, context):
    # Get all the clicks from event's content
    clicks_array  = []
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    logger.info('Reading key: {}, from bucket: {}'.format(key, bucket))
    
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + obj['ContentType'])
        lines = obj['Body'].read().split(b'\n')
        for r in lines:
            click = r.decode()
            logger.info(click)
            clicks_array.append(click)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
    
    
    # Update DynamoDB campaign budgets for every click
    for click_str in clicks_array:
        dynamo_response = update_campaign(click_str)
        logger.error(dynamo_response)
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def update_campaign(click_str):
    table = dynamodb.Table('budgets')
    click_dict = ast.literal_eval(click_str)
    campaign_id = click_dict['campaign_id']
    previous_campaign_balance = get_previous_campaign_balance(table, campaign_id)
    new_campaign_balance = previous_campaign_balance - float(click_dict['publisher_price'])
    try:
        logger.info('campaign_id: {}, new balance: {}'.format(campaign_id, new_campaign_balance))
        response = table.update_item(
            Key={'campaign_id': campaign_id},
            UpdateExpression='SET balance = :val1',
            ExpressionAttributeValues={':val1': new_campaign_balance}
        )
    except ClientError:
        logger.exception("Couldn't load data into table %s.", table.name)
        raise
    return response


def get_previous_campaign_balance(table, campaign_id):
    try:
        response = table.get_item(
            Key={'campaign_id': campaign_id}
        )
        item = response['Item']
    except ClientError:
        logger.exception("Couldn't load data into table %s.", table.name)
        raise
    return float(item['balance'])

