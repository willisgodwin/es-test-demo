es_test_mapping = {
  "settings": {
    "number_of_shards": 2,
    "number_of_replicas": 1,
    "analysis": {
      "analyzer": {
        "my_english_analyzer": {
          "type": "standard",
          "stopwords": "_english_"
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "no": {"type": "integer"},
      "age": {"type": "integer"},
      "gender": {"type": "integer"},
      "salary": {"type": "float"},
      "monthly_expenditure": {"type": "float"},
      "occupation": {"type": "text"},
      "healthy_lifestyle": {"type": "keyword"}
    }
  }
}
