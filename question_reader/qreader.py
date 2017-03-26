
import copy
import json
import pprint
import re

date_field_phrases = [
    "For last <7d/24h/60m>",
    "Between time <now-48h> and <now-24h>"
]

numeric_field_phrases = [
    "return average for",
    "return sum of",
    "return min of",
    "return max of"
]

string_field_phrases = [
    "where % = <x>",
    "For every"
]


date_query_part = {
    "query": {
        "bool": {
            "must": [
                {
                    "range": {
                        "timestamp": {
                            "format": "epoch_millis"
                        }
                    }
                }
            ]
        }
    }
}

numeric_query_part = {
    "aggs": {
        "numeric": {
        }
    }
}

filter_query_part = {
    "aggs": {
        "string_field_agg": {
            "filter": {
                "term": {
                }
            }
        }
    }
}

terms_query_part = {
    "aggs": {
        "string_field_agg": {
            "terms": {}
        }
    }
}

NUMERIC_OP = {"average": "avg", "sum": "sum", "min": "min", "max": "max"}


class QuestionReader():
    def __init__(self, conn_handle, question):
        self.conn_handle = conn_handle
        self.question = question

    def generate_result(self):
        if self.conn_handle.type == 'elasticsearch':
            query, index = self._es_prepare_query()
            return json.dumps(self._es_response(self.conn_handle.handle,
                                                query, index))

    def _es_prepare_query(self):
        string_query = None
        if self.question.startswith("For last"):
            x = self.question.split(" ")[2]
            x = re.compile("([0-9]+)([a-zA-Z]+)").match(x)
            x = x.group(1) + x.group(2)[:1]
            date_query = copy.deepcopy(date_query_part)
            (date_query["query"]["bool"]["must"][0]["range"]
                ["timestamp"]["lte"]) = "now"
            (date_query["query"]["bool"]["must"][0]["range"]
                ["timestamp"]["gte"]) = "now-" + x
            # print date_query
        elif self.question.startswith("Between time"):
            x = self.question.split(" ")[2]
            y = self.question.split(" ")[4]
            date_query = copy.deepcopy(date_query_part)
            (date_query["query"]["bool"]["must"][0]["range"]
                ["timestamp"]["lte"]) = x
            (date_query["query"]["bool"]["must"][0]["range"]
                ["timestamp"]["gte"]) = y
            # print date_query

        for phrase in numeric_field_phrases:
            if phrase in self.question:
                    operation = self.question.split(
                        "return", 1)[1].split(" ")[1]
                    field = self.question.split(
                        "return", 1)[1].split(" ")[3]
                    numeric_query = copy.deepcopy(numeric_query_part)
                    (numeric_query["aggs"]
                        ["numeric"][NUMERIC_OP[operation]]) = {"field": field}
            # print numeric_query

        for phrase in string_field_phrases:
            if ("%" in phrase and phrase.split("%")[0] in self.question and
                    "=" in self.question):
                filter_term = self.question.split("where", 1)[1].split(" ")[1]
                filter_value = self.question.split("where", 1)[1].split(" ")[3]
                string_query = copy.deepcopy(filter_query_part)
                (string_query["aggs"]["string_field_agg"]
                    ["filter"]["term"]) = {filter_term: filter_value}
            elif phrase in self.question:
                field = self.question.split(phrase, 1)[1].split(" ")[1]
                string_query = copy.deepcopy(terms_query_part)
                (string_query["aggs"]
                    ["string_field_agg"]["terms"]) = {"field": field}

        if "Index reference" in self.question:
            index = self.question.split("Index reference", 1)[1].split(" ")[1]

        query_dict = {}
        query_dict.update(date_query)
        if string_query:
            string_query["aggs"]["string_field_agg"].update(numeric_query)
            query_dict.update(string_query)
        else:
            query_dict.update(numeric_query)
        query_dict.update({"size": 0})
        # pprint.pprint(query_dict)
        return query_dict, index

    def _es_response(self, handle, query, index):
        return handle.search(index=index, body=query)
