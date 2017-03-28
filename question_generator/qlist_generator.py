import itertools

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


class QuestionGenerator():
    def __init__(self, schema, db_conn):
        self.schema = schema
        self.connector_type = db_conn.type
        self.question_list = []

    def generate(self):
        if self.connector_type == "elasticsearch":
            return self.es_question_generator(self.schema)

    def _es_question_phraser(self, question_phrases, count, index):
        for phrase in question_phrases:
            question = ""
            for i in range(count):
                question += phrase[i] + " "
            question += " ( Index reference: " + index + " )"
            # print question
            self.question_list.append(question)

    def _es_num_phrase_generator(self, item):
        if item[1].get("numeric_fields") is not None:
            question_phrases = itertools.product(numeric_field_phrases,
                                                 item[1].get("numeric_fields"))
            self._es_question_phraser(question_phrases, 2, item[0])

    def _es_num_and_date_phrase_generator(self, item):
        if (item[1].get("date_field") is not None and
                item[1].get("numeric_fields") is not None):
            question_phrases = itertools.product(
                date_field_phrases,
                numeric_field_phrases,
                item[1].get("numeric_fields"))
            self._es_question_phraser(question_phrases, 3, item[0])

    def _es_str_phrase_generator(self, item):
        if item[1].get("string_fields") is not None:
            question_phrases = itertools.product(string_field_singular_phrases,
                                                 item[1].get("string_fields"))
            self._es_question_phraser(question_phrases, 2, item[0])

    def _es_str_and_date_phrase_generator(self, item):
        if (item[1].get("date_field") is not None and
                item[1].get("string_fields") is not None):
            question_phrases = itertools.product(
                date_field_phrases,
                string_field_singular_phrases,
                item[1].get("string_fields"))
            self._es_question_phraser(question_phrases, 3, item[0])

    def _es_str_num_and_date_phrase_generator(self, item):
        if (item[1].get("date_field") is not None and
                item[1].get("string_fields") is not None and
                item[1].get("date_field") is not None):
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
                self.question_list.append(question)

    def es_question_generator(self, schema):
        for schema_item in schema:
            for item in schema_item.iteritems():
                self._es_num_phrase_generator(item)
                self._es_num_and_date_phrase_generator(item)
                self._es_str_phrase_generator(item)
                self._es_str_and_date_phrase_generator(item)
                self._es_str_num_and_date_phrase_generator(item)
        print("%d total questions generated" % len(self.question_list))
        return self.question_list
