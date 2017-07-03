#!/usr/bin/env python
import argparse
import copy

from flask import Flask
from flask import redirect, render_template, request, session, url_for
import yaml

from db_connector import connector as conn
from question_generator import qlist_generator as qg
from question_reader import qreader
from schema_reader import schemareader

db_conn = None
app = Flask(__name__)
app.secret_key = (
    '\x16T@g\xcf\xdcGRzn\xf5\xc4\x068\xb9\xf7\xf7r]\xf6d\x96\x9a\xdd')

special_query = {
    "Number of transactions for every branch": {
        "query": {
            "size": 0,
            "aggs": {
                "transaction": {
                    "filters": {
                        "filters": {
                            "withdrawal": {"match": {"transaction_type": "Withdrawal"}},
                            "deposit": {"match": {"transaction_type": "Deposit"}}
                        }
                    },
                    "aggs": {
                        "transaction_by_branch": {
                            "terms": { "field": "branch"}
                        }
                    }
                }
            }
        }
    },
    "Top 5 customers by balance": {
        "query": {
            "size": 5,
            "sort": [
                {"balance": {"order": "desc"}}
            ],
            "_source": ["balance", "customer", "branch", "timestamp"]
        }
    },
    "Maximum Withdrawal": {
        "query": {
            "size": 1,
            "sort": [
                {"amount": {"order": "desc"}}
            ],
            "_source": ["customer", "amount", "branch"],
            "query": {
                "term": {"transaction_type": "Withdrawal"}
            }
        }
    },
    "Maximum Deposit": {
        "query": {
            "size": 1,
            "sort": [
                {"amount": {"order": "desc"}}
            ],
            "_source": ["customer", "amount", "branch"],
            "query": {
                "term": {"transaction_type": "Deposit"}
            }
        }
    }
}


def configRead():
    config_dict = {}
    with open("config.yml", "r") as configread:
        try:
            config_dict = yaml.load(configread)
        except yaml.YAMLError as exc:
            print(exc)
    return config_dict


@app.route("/questions", methods=["POST", "GET"])
def question_page():
    if request.method == "POST":
        print request.form["question"]
        question_reader = qreader.QuestionReader(
            db_conn, request.form["question"])
        result = question_reader.generate_result()
        return result;
    return render_template("question-list.html",
                           question_list=session['question_list'])


@app.route("/", methods=["POST", "GET"])
def connection_info():
    error = None
    global db_conn
    if request.method == "POST":
        config_dict = {}
        config_dict['db.type'] = request.form['connector-type']
        config_dict['db.hostname'] = request.form['hostname']
        # connect to DB
        db_conn = conn.DBConnector(config_dict)
        db_conn.connect()
        print "Connection established."
        schema_reader = schema_read(db_conn)
        print "Schema Read"
        session['question_list'] = question_generator(schema_reader, db_conn)
        # Special questions for the demo only
        special_query_list = []
        sq_reader = qreader.QuestionReader(
                                    db_conn, None)
        global special_query
        special_query_item = {}
        for question, query_item in special_query.iteritems():
            response = (
                sq_reader.special_query_es_response(db_conn,
                    query_item.get("query")))
            special_query_item['question'] = question
            special_query_item['response'] = response
            special_query_list.append(copy.deepcopy(special_query_item))
        print special_query_list
        session['special_query_list'] = special_query_list
        print "Questions generated"
        return redirect(url_for(".question_page"))
    return render_template("conn-info.html", error=error)


def schema_read(db_conn):
    # Read schema
    schema_reader = schemareader.SchemaReader(db_conn)
    schema_reader.read()
    return schema_reader


def question_generator(schema_reader, db_conn):
    # Generate questions based on reading the schema
    qg_handle = qg.QuestionGenerator(schema_reader.schema, db_conn)
    question_list = qg_handle.generate()
    return question_list


if __name__ == "__main__":
    app.run()
