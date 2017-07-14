#!/usr/bin/env python

import os
import time
from slackclient import SlackClient
import yaml

from db_connector import connector as conn
from question_generator import qlist_generator as qg
from question_reader import qreader
from schema_reader import schemareader


# starterbot's ID as an environment variable
BOT_ID = os.environ.get("BOT_ID")
CONFIG_DICT = {}
AT_BOT = "<@" + BOT_ID + ">"
question_list = []
db_conn = None

# instantiate Slack & Twilio clients
slack_client = SlackClient(os.environ.get('SLACK_BOT_TOKEN'))


def handle_command(command, channel):
    """
        Receives commands directed at the bot and determines if they
        are valid commands. If so, then acts on the commands. If not,
        returns back what it needs for clarification.
    """
    if command == "question list":
        if len(question_list) > 0:
            response = ""
        else:
            response = "I dont have any list"
        id = 1
        for question in question_list:
            response += str(id) + ". " + question + "\n"
            id +=1
        slack_client.api_call("chat.postMessage", channel=channel,
                          text=response, as_user=True)
        response = "What question would you like to ask (type: question no 1 for ex)"
        slack_client.api_call("chat.postMessage", channel=channel,
                          text=response, as_user=True)
    elif command.startswith("question no"):
        qno = int(command.split("question no")[1].strip())
        question_reader = qreader.QuestionReader(
            db_conn, question_list[qno-1])
        response = question_reader.generate_result()
        slack_client.api_call("chat.postMessage", channel=channel,
                          text=response, as_user=True)
    elif command == "hi" or command == "hello" or command == "help":
        response = "Hi\n"
        response += "Type one of the following\n"
        response += "question list\n"
        response += "question no <no> \n"
        slack_client.api_call("chat.postMessage", channel=channel,
                          text=response, as_user=True)
    else:
        response = "I cannot understand your question"
        slack_client.api_call("chat.postMessage", channel=channel,
                          text=response, as_user=True)


def parse_slack_output(slack_rtm_output):
    """
        The Slack Real Time Messaging API is an events firehose.
        this parsing function returns None unless a message is
        directed at the Bot, based on its ID.
    """
    output_list = slack_rtm_output
    if output_list and len(output_list) > 0:
        for output in output_list:
            if output and 'text' in output and AT_BOT in output['text']:
                # return text after the @ mention, whitespace removed
                return output['text'].split(AT_BOT)[1].strip().lower(), \
                       output['channel']
    return None, None

def question_generator(schema_reader, db_conn):
    # Generate questions based on reading the schema
    qg_handle = qg.QuestionGenerator(schema_reader.schema, db_conn)
    question_list = qg_handle.generate()
    return question_list

def schema_read(db_conn):
    # Read schema
    schema_reader = schemareader.SchemaReader(db_conn)
    schema_reader.read()
    return schema_reader

def serverStartup():
    global question_list, db_conn
    db_conn = conn.DBConnector(CONFIG_DICT)
    db_conn.connect()
    print "Connection established."
    schema_reader = schema_read(db_conn)
    print "Schema Read"
    question_list = question_generator(schema_reader, db_conn)
    print "Questions generated"

def configRead():
    global CONFIG_DICT
    with open("config.yml", "r") as fileread:
        try:
            CONFIG_DICT = yaml.load(fileread)
        except yaml.YAMLError as exc:
            print(exc)

if __name__ == "__main__":
    configRead()
    serverStartup()
    READ_WEBSOCKET_DELAY = 1 # 1 second delay between reading from firehose
    if slack_client.rtm_connect():
        print("Params-di-bot connected and running!")
        while True:
            command, channel = parse_slack_output(slack_client.rtm_read())
            if command and channel:
                handle_command(command, channel)
            time.sleep(READ_WEBSOCKET_DELAY)
    else:
        print("Connection failed. Invalid Slack token or bot ID?")