#!/usr/bin/env python
"""
Client which receives and processes the requests
"""
import os
import logging
import argparse
import urllib2
from flask import Flask, request
from flask_dynamo import Dynamo

# configure logging
logging.basicConfig(level=logging.INFO)

# environment vars
API_TOKEN = os.getenv("GD_API_TOKEN")
if API_TOKEN is None:
    raise Exception("Must define GD_API_TOKEN environment variable")
API_BASE = os.getenv("GD_API_BASE")
if API_BASE is None:
    raise Exception("Must define GD_API_BASE environment variable")

#MESSAGES = {}

app = Flask(__name__)
app.config.update(
  DEBUG=True,
  AWS_REGION='us-west-2'
)

app.config['DYNAMO_TABLES'] = [
    {
         TableName='messages',
         KeySchema=[dict(AttributeName='msg_id', KeyType='HASH')],
         AttributeDefinitions=[dict(AttributeName='msg_id', AttributeType='S')],
         ProvisionedThroughput=dict(ReadCapacityUnits=5, WriteCapacityUnits=5)
    }
 ]

dynamo = Dynamo()
dynamo.init_app(app)
dynamo.create_all()

# creating flask route for type argument
@app.route('/', methods=['GET', 'POST'])
def main_handler():
    """
    main routing for requests
    """
    if request.method == 'POST':
        return process_message(request.get_json())
    else:
        return get_message_stats()

def get_message_stats():
    """
    provides a status that players can check
    """
    msg_count = len(MESSAGES.keys())
    return "There are {} messages in the MESSAGES dictionary".format(msg_count)

def process_message(msg):
    """
    processes the messages by combining parts
    """
    msg_id = msg['Id'] # The unique ID for this message
    part_number = msg['PartNumber'] # Which part of the message it is
    data = msg['Data'] # The data of the message

    # log
    logging.info("Processing message for msg_id={} with part_number={} and data={}".format(msg_id, part_number, data))

    # Try to get the parts of the message from the MESSAGES dictionary.
    # If it's not there, create one that has None in both parts

    table = dynamo.tables['messages']
    item = {'first': None, 'second': None}

    try:
        response = table.get_item(
            Key={
                'msg_id': msg_id
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        item = response['Item']
        print("GetItem succeeded:")
        print(json.dumps(item, indent=4, cls=DecimalEncoder))

    # parts = MESSAGES.get(msg_id, [None, None])

    # # store this part of the message in the correct part of the list
    # parts[part_number] = data

    # # store the parts in MESSAGES
    # MESSAGES[msg_id] = parts

    if part_number == 0:
        response = table.update_item(
            Key={
                'msg_id': msg_id,
            },
            UpdateExpression="set first = :r",
            ExpressionAttributeValues={
                ':r': data,
            },
            ReturnValues="UPDATED_NEW"
        )
        first_part = data
        second_part = item.second
    elif part_number == 1:
        response = table.update_item(
            Key={
                'msg_id': msg_id,
            },
            UpdateExpression="set second = :r",
            ExpressionAttributeValues={
                ':r': data,
            },
            ReturnValues="UPDATED_NEW"
        )
        first_part = item.first
        second_part = data

    print("UpdateItem succeeded:")
    print(json.dumps(response, indent=4, cls=DecimalEncoder))

    # if both parts are filled, the message is complete
    if None not in parts:
        # app.logger.debug("got a complete message for %s" % msg_id)
        logging.info("Have both parts for msg_id={}".format(msg_id))
        # We can build the final message.
        # result = parts[0] + parts[1]
        result = first_part + second_part
        logging.debug("Assembled message: {}".format(result))
        # sending the response to the score calculator
        # format:
        #   url -> api_base/jFgwN4GvTB1D2QiQsQ8GHwQUbbIJBS6r7ko9RVthXCJqAiobMsLRmsuwZRQTlOEW
        #   headers -> x-gameday-token = API_token
        #   data -> EaXA2G8cVTj1LGuRgv8ZhaGMLpJN2IKBwC5eYzAPNlJwkN4Qu1DIaI3H1zyUdf1H5NITR
        url = API_BASE + '/' + msg_id
        logging.debug("Making request to {} with payload {}".format(url, result))
        req = urllib2.Request(url, data=result, headers={'x-gameday-token':API_TOKEN})
        resp = urllib2.urlopen(req)
        logging.debug("Response from server: {}".format(resp.read()))
        resp.close()

    return 'OK'

if __name__ == "__main__":
    # By default, we disable threading for "debugging" purposes.
    app.run(host="0.0.0.0", port="5000", threaded=True)
