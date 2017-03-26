
import copy

ES_EXCLUDE_INDEX = [".kibana"]
ES_NUMERIC_FIELD_LIST = ["long", "integer", "short",
                         "byte", "double", "float"]


class SchemaReader():
    def __init__(self, conn_handle):
        self.conn_handle = conn_handle
        self.schema = None

    def read(self):
        if self.conn_handle.type == 'elasticsearch':
            self._es_read(self.conn_handle.handle)

    def _es_read(self, handle):
        cat_indices = handle.cat.indices().split('\n')
        index_list = []
        index_field_list = []
        # Example index_field_list
        # [{"index1": {"alias": "index1-alias",
        #   "string_fields": ["v1", "v2"], "numeric_fields": ['v3', 'v4'],
        #   "date_field": ["v5"]}},
        #  {"index2", {...}}]

        for line in cat_indices:
            if line:
                index_list.append(line.split(' ')[2])

        for index in index_list:
            if index in ES_EXCLUDE_INDEX:
                continue
            string_fields = []
            numeric_fields = []
            index_mapping = (
                handle.indices.get_mapping(index)[index]['mappings'])
            field_dict = {}
            date_field = None
            for type in index_mapping.iteritems():
                fields = type[1].get("properties")
                for field in fields.iteritems():
                    object_field = field[1].get("properties")
                    if object_field is not None:
                        for item in object_field.iteritems():
                            if item[1].get("type") == "string":
                                string_fields.append(field[0] + "." + item[0])
                            elif item[1].get("type") in ES_NUMERIC_FIELD_LIST:
                                numeric_fields.append(field[0] + "." + item[0])
                            elif item[1].get("type") == "date":
                                date_field = field[0] + "." + item[0]
                    else:
                        if field[1].get("type") == "string":
                            string_fields.append(field[0])
                        elif field[1].get("type") in ES_NUMERIC_FIELD_LIST:
                            numeric_fields.append(field[0])
                        elif field[1].get("type") == "date":
                            date_field = field[0]

            index_field_list.append({index: {"alias": index,
                                             "string_fields": string_fields,
                                             "numeric_fields": numeric_fields,
                                             "date_field": date_field}})
        self.schema = index_field_list
