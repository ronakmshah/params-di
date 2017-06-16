
import copy
import json
import pprint
import re

DEMO_INDEX = "demo_transaction"

date_field_phrases = [
    "For last <7d/24h/60m>",
    "Between time <now-48h> and <now-24h>"
]

numeric_field_phrases = [
    "return average for",
    "return sum of",
    "return min of",
    "return max of",
    "return percentile of",
    "return stats(min,max,sum,count,avg) for"
]

string_field_phrases = [
    "where % = <x>",
    "For every"
]

string_field_singular_phrases = [
    "return unique "
]

bool_query_part = {
    "query": {
        "bool": {
            "must": [
            ]
        }
    }
}

date_query_part = {
    "range": {
        "timestamp": {
            "format": "epoch_millis"
        }
    }
}

term_query_part = {
    "term": {}
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

cardinality_query_part = {
    "aggs": {
        "unique": {
            "cardinality": {}
        }
    }
}

NUMERIC_OP = {
    "average": "avg",
    "sum": "sum",
    "min": "min",
    "max": "max",
    "percentile": "percentiles",
    "stats(min,max,sum,count,avg)": "stats"
}


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
        numeric_query = None
        bool_query = None
        date_query = None

        if self.question.startswith("For last"):
            x = self.question.split(" ")[2]
            x = re.compile("([0-9]+)([a-zA-Z]+)").match(x)
            #x = x.group(1) + x.group(2)[:1]
            x = "30d"
            date_query = copy.deepcopy(date_query_part)
            bool_query = copy.deepcopy(bool_query_part)
            (date_query["range"]
                ["timestamp"]["lte"]) = "now"
            (date_query["range"]
                ["timestamp"]["gte"]) = "now-" + x
            bool_query["query"]["bool"]["must"].append(date_query)
            # print date_query
        elif self.question.startswith("Between time"):
            x = self.question.split(" ")[2]
            y = self.question.split(" ")[4]
            date_query = copy.deepcopy(date_query_part)
            bool_query = copy.deepcopy(bool_query_part)
            (date_query["range"]
                ["timestamp"]["lte"]) = x
            (date_query["range"]
                ["timestamp"]["gte"]) = y
            bool_query["query"]["bool"]["must"].append(date_query)

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
            #print numeric_query

        for phrase in string_field_phrases:
            if ("%" in phrase and phrase.split("%")[0] in self.question and
                    "=" in self.question):
                filter_term = self.question.split("where", 1)[1].split(" ")[1]
                filter_value = self.question.split("where", 1)[1].split(" ")[3]
                if not bool_query:
                    bool_query = copy.deepcopy(bool_query_part)
                term_query = copy.deepcopy(term_query_part)
                term_query['term'] = {filter_term: filter_value}
                bool_query["query"]["bool"]["must"].append(term_query)
            elif phrase in self.question:
                field = self.question.split(phrase, 1)[1].split(" ")[1]
                string_query = copy.deepcopy(terms_query_part)
                (string_query["aggs"]
                    ["string_field_agg"]["terms"]) = {"field": field, "size": 0}

        for phrase in string_field_singular_phrases:
            if (phrase in self.question):
                index = self.question.find(phrase) + len(phrase)
                field = self.question[index:].split(' ')[0]
                string_query = copy.deepcopy(cardinality_query_part)
                (string_query["aggs"]
                    ["unique"]["cardinality"]) = {"field": field}
                #print string_query

        if "Index reference" in self.question:
            index = self.question.split("Index reference", 1)[1].split(" ")[1]
        else:
            index = DEMO_INDEX

        query_dict = {}
        if bool_query:
            query_dict.update(bool_query)
        if string_query:
            if numeric_query:
                string_query["aggs"]["string_field_agg"].update(numeric_query)
            query_dict.update(string_query)
        else:
            query_dict.update(numeric_query)
        query_dict.update({"size": 0})
        print query_dict
        # pprint.pprint(query_dict)
        return query_dict, index

    def _es_response(self, handle, query, index):
        return handle.search(index=index, body=query)

    def special_query_es_response(self, handle, query, index=None):
        index = index if index else DEMO_INDEX
        return json.dumps(self._es_response(handle.handle, query, index))
