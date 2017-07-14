#!/bin/sh
curl -XPUT 'http://localhost:9200/_template/demo_template' -d '
{
    "order" : 0,
    "template" : "demo_*",
    "settings" : { },
    "mappings" : {
      "demo_type" : {
        "dynamic_templates" : [ {
          "strings" : {
            "mapping" : {
              "index" : "not_analyzed",
              "type" : "string"
            },
            "match_mapping_type" : "string"
          }
        }, {
          "timestamps" : {
            "mapping" : {
              "type" : "date"
            },
            "match" : "timestamp"
          }
        } ]
      }
    },
    "aliases" : { }
}
'