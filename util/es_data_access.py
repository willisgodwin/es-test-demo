import json
import logging
import numpy as np
import pandas as pd
from config import settings
from elasticsearch import Elasticsearch
from exceptions import ElasticSearchFailure, ESDeleteIndexFailure
from typing import Dict

logger = logging.getLogger(__name__)


class ElasticSearchManagement:
    def __init__(self):
        self.es_connection_string = settings.ES_CONNECTION_STRING
        self.es_index_name = settings.ES_INDEX_NAME
        self.es_client = Elasticsearch(settings.ES_CONNECTION_STRING)
        self.ping()

    def ping(self) -> bool:
        result = self.es_client.ping()
        logging.info(result)
        if not result:
            raise ElasticSearchFailure("results were not returned from elasticsearch.")

        return result

    def _index_exists(self) -> bool:
        result = False
        try:
            response = self.es_client.indices.exists(index=f"{self.es_index_name}")
            if response and response.meta and response.meta.status == 200:
                result = True
        except Exception as e:
            logging.exception(e)

        return result

    def _delete_index(self):
        """
        delete an elasticsearch index
        :return: bool whether the operation was successful
        """
        result = False
        try:
            response = self.es_client.indices.delete(index=self.es_index_name)
            if response and response.meta and response.meta.status == 200:
                result = True
            else:
                raise ElasticSearchFailure(f"Failed to delete the index:'{self.es_index_name}'")
        except Exception as e:
            logging.error(str(e))

        return result

    def create_index(self, mapping: Dict) -> bool:
        """
        Create an es_client index.
        :param mapping: Mapping of the index
        """
        result = False
        try:
            if self._index_exists():
                logging.info(f"index: '{self.es_index_name}' already exists.")
                logging.info(f"deleting index: '{self.es_index_name}'.")
                delete_result = self._delete_index()
                if not delete_result:
                    raise ESDeleteIndexFailure("failed to delete the index")

            logging.info(
                f"Creating index {self.es_index_name} with the following schema: {json.dumps(mapping, indent=2)}")

            response = self.es_client.indices.create(index=self.es_index_name, ignore=400, body=mapping)
            if response and response.meta and response.meta.status == 200:
                result = True
        except Exception as e:
            logging.exception(e)

        return result

    def populate_index(self, path: str) -> None:
        """
        Populate an index from a CSV file.
        :param path: The path to the CSV file.
        """
        try:
            df = pd.read_csv(path).replace({np.nan: None})
            logging.info(f"Writing {len(df.index)} documents to ES index {self.es_index_name}")
            for doc in df.apply(lambda x: x.to_dict(), axis=1):
                self.es_client.index(index=self.es_index_name, body=json.dumps(doc))
        except Exception as e:
            logging.exception(e)
        finally:
            # this forces the index to refresh the visible data.
            # it will handle this on its own but at its own speed, but we want the data right now.
            self.es_client.indices.refresh(index=self.es_index_name)


class ElasticSearchDataAccess:
    def __init__(self):
        self.es_connection_string = settings.ES_CONNECTION_STRING
        self.es_index_name = settings.ES_INDEX_NAME
        self.es_client = Elasticsearch(settings.ES_CONNECTION_STRING)
        self.ping()

    def ping(self) -> bool:
        result = self.es_client.ping()
        if not result:
            raise ElasticSearchFailure(f"Index: '{self.es_index_name}' does not exist")
        return result

    def search(self,
               no_filter: int = None,
               gender_filter: str = None,
               occupation_filter: [] = None,
               salary_range_filter_lt: float = None,
               salary_range_filter_gt: float = None,
               monthly_expenditure_range_filter_lt: float = None,
               monthly_expenditure_range_filter_gt: float = None,
               healthy_lifestyle_filter: [] = None,
               ):
        """
        This is the method one would use to pass any number of key/value pair search args to retrieve results.

        returns: search result of all matching documents
        """

        filter_and_list = list()
        if no_filter:
            filter_and_list.append({"key": "no", "value": no_filter})

        if gender_filter:
            filter_and_list.append({"key": "gender", "value": gender_filter})

        # filter_or_list = list()
        # if occupation_filter:
        #     filter_or_list.append({"key": "occupation", "value": occupation_filter})
        #
        # if healthy_lifestyle_filter:
        #     filter_or_list.append({"key": "healthy_lifestyle", "value": healthy_lifestyle_filter})

        must_filters = list()
        if filter_and_list and len(filter_and_list) > 0:
            for filter in filter_and_list:
                str_filter = '{"match": {"' + filter["key"] + '":"' + str(filter["value"]) + '"}}'
                must_filters.append(str_filter)

            print(','.join(must_filters))

        # should_filters = None
        # if filter_or_list and len(filter_or_list) > 0:
        #     should_filters = '{"should": [{0}]}'.format(join(filter_or_list, ','))

        if len(must_filters) > 0:
            query_body = '{"query": {"bool":{"must": [' + ",".join(must_filters) + ']}}}'
        else:
            query_body = '{"query": {"match_all":{}}'

        json_query_body = json.loads(query_body)
        result = self.es_client.search(index="_all", request_timeout=5, body=json_query_body)
        if not result:
            raise ElasticSearchFailure("results were not returned from elasticsearch.")

        logging.info(f'query hits: {result["hits"]["hits"]} ')
        return result

    def get(self, no: int):
        """
        This function will get 1 document by 'no' -> id

        no: int this is the employee number to filter by

        returns: search result of one record
        """
        query_body = {
            "query": {
                "bool": {
                    "must": {
                        "match": {
                            "no": no
                        }
                    }
                }
            }
        }

        result = self.es_client.search(index="_all", body=query_body, request_timeout=5)
        if not result:
            raise ElasticSearchFailure("results were not returned from elasticsearch.")

        logging.info(f'query hits: {result["hits"]["hits"]} ')

        return result
