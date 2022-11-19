es_test_mapping = {
  "settings": {
    "analysis": {
      "analyzer": {
        "rebuild_standard": {
          "tokenizer": "standard",
          "filter": [
            "lowercase", "asciifolding"
          ]
        }
      }
    },
    "number_of_shards": 2,
    "number_of_replicas": 1,
  },
  "mappings": {
    "properties": {
      "no": {"type": "integer"},
      "gender": {"type": "integer"},
      "salary": {"type": "float"},
      "monthly_expenditure": {"type": "float"},
      "occupation": {"type": "text", "analyzer": "rebuild_standard"},
      "healthy_lifestyle": {"type": "keyword"}
    }
  }
}
