# Params Data Insight

## About:
Idea behind this application is to implement self-service data intelligence.
1. Connect to your datastore.
2. Get to know all the possible questions in a natural language that your data can answer.
3. Ask the question and get back the answer.

## Setup:
1. `git clone` this repository and go to params-di folder.
2. `pip install -r requirements.txt`
3. Setup flask app environment variable
   `export FLASK_APP=params-di.py`
4. Run the server:
   flask run [--host=<ip>]
5. Go to http://<ip:5000> on web browser and start using application

## Sample Question-Answer:
Question: For last 90d return average for value where product = "bat" ( Index reference: sales )
Answer:
Elasticsearch response:
`
{"hits": {"hits": [], "total": 90, "max_score": 0.0}, "_shards": {"successful": 5, "failed": 0, "total": 5}, "took": 124, "aggregations": {"string_field_agg": {"numeric": {"value": 140}, "doc_count": 90}}, "timed_out": false}
`

## TODO:
1. Tabify the response for better visualization
2. Handling of elasticsearch's error response
3. Optimization of the questions generated.