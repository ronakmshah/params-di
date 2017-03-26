#!/usr/bin/env python
import argparse

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
        return render_template("question-list.html", result=str(result))
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
