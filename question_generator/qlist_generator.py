import itertools

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


class QuestionGenerator():
    def __init__(self, schema, db_conn):
        self.schema = schema
        self.connector_type = db_conn.type

    def generate(self):
        if self.connector_type == "elasticsearch":
            return self.es_question_generator(self.schema)

    def es_question_generator(self, schema):
        question_list = []
        for schema_item in schema:
            for item in schema_item.iteritems():
                if item[1].get("date_field") is not None:
                    date_phrase = True
                if item[1].get("numeric_fields") is not None:
                    numeric_fields = True
                    if date_phrase is True:
                        question_phrases = itertools.product(
                            date_field_phrases,
                            numeric_field_phrases,
                            item[1].get("numeric_fields"))
                    else:
                        question_phrases = numeric_field_phrases
                    for phrase in question_phrases:
                        question = (phrase[0] + " " + phrase[1] + " " +
                                    phrase[2] + " (Index reference: " +
                                    item[0] + " )")
                        question_list.append(question)
                if item[1].get("string_fields") is not None:
                    if date_phrase is True and numeric_fields is True:
                        question_phrases = itertools.product(
                            date_field_phrases, numeric_field_phrases,
                            item[1].get("numeric_fields"),
                            string_field_phrases,
                            item[1].get("string_fields"))
                    for phrase in question_phrases:
                        if "%" in phrase[3]:
                            question = (" ".join([phrase[0],
                                        phrase[1], phrase[2],
                                        phrase[3].split("%")[0],
                                        phrase[4], phrase[3].split("%")[1]]))
                        else:
                            question = (" ".join([phrase[0],
                                        phrase[1], phrase[2],
                                        phrase[3], phrase[4]]))
                        question = (question +
                                    " (Index reference: " + item[0] + " )")
                        question_list.append(question)
        return question_list
