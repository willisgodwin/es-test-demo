import json
import logging
from typing import Dict

import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch

from config import settings
from exceptions import ElasticSearchFailure, ESDeleteIndexFailure
from schema.model import EmployeeSearchFilter

logger = logging.getLogger(__name__)


class ElasticSearchManagement:
    def __init__(self):
        self.es_connection_string = settings.ES_CONNECTION_STRING
        self.es_index_name = settings.ES_INDEX_NAME
        self.es_client = Elasticsearch(settings.ES_CONNECTION_STRING)
        self.ping()

    def ping(self) -> bool:
        """
        This will just ping the cluster to ensure it's up
        :return: true if the cluster is up.
        """
        result = self.es_client.ping()
        logging.info(result)
        if not result:
            raise ElasticSearchFailure("results were not returned from elasticsearch.")

        return result

    def _index_exists(self) -> bool:
        """
        checks if the index exists
        :return: true if the index exists
        """
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

    def populate_index(self, path: str) -> bool:
        """
        Populate an index from a CSV file.
        :param path: The path to the CSV file.
        """
        result = False
        try:
            df = pd.read_csv(path).replace({np.nan: None})
            logging.info(f"Writing {len(df.index)} documents to ES index {self.es_index_name}")
            for doc in df.apply(lambda x: x.to_dict(), axis=1):
                self.es_client.index(index=self.es_index_name, body=json.dumps(doc))

            result = True
        except Exception as e:
            logging.exception(e)
        finally:
            # this forces the index to refresh the visible data.
            # it will handle this on its own but at its own speed, but we want the data right now.
            self.es_client.indices.refresh(index=self.es_index_name)

        return result


class ElasticSearchDataAccess:
    def __init__(self):
        self.es_connection_string = settings.ES_CONNECTION_STRING
        self.es_index_name = settings.ES_INDEX_NAME
        self.es_client = Elasticsearch(settings.ES_CONNECTION_STRING)
        self.ping()

    def ping(self) -> bool:
        """
        This will just ping the cluster to ensure it's up
        :return: true if the cluster is up
        """
        result = self.es_client.ping()
        if not result:
            raise ElasticSearchFailure(f"Index: '{self.es_index_name}' does not exist")
        return result

    def search(self, employee_search_filter: EmployeeSearchFilter):
        """
        This is the method one would use to pass any number of key/value pair search args to retrieve results.

        :returns search result of all matching documents
        :param employee_search_filter: the filters to be applied to the query
        """

        term_filter_and_list = list()
        if employee_search_filter.age_filter:
            term_filter_and_list.append({"key": "age", "value": employee_search_filter.age_filter})

        if employee_search_filter.gender_filter:
            term_filter_and_list.append({"key": "gender", "value": employee_search_filter.gender_filter})

        if employee_search_filter.occupation_filter:
            term_filter_and_list.append({"key": "occupation", "value": employee_search_filter.occupation_filter})

        range_filter_and_list = list()
        if employee_search_filter.salary_range_filter_gte or \
                employee_search_filter.salary_range_filter_lte:
            range_filter_and_list.append(
                {
                    "key": "salary",
                    "gte": employee_search_filter.salary_range_filter_gte if employee_search_filter.salary_range_filter_gte else None,
                    "lte": employee_search_filter.salary_range_filter_lte if employee_search_filter.salary_range_filter_lte else None
                })

        if employee_search_filter.monthly_expenditures_range_filter_lte or \
                employee_search_filter.monthly_expenditures_range_filter_gte:
            range_filter_and_list.append(
                {
                    "key": "monthly_expenditures",
                    "gte": employee_search_filter.monthly_expenditures_range_filter_gte if employee_search_filter.monthly_expenditures_range_filter_gte else None,
                    "lte": employee_search_filter.monthly_expenditures_range_filter_lte if employee_search_filter.monthly_expenditures_range_filter_lte else None
                })

        # filter_or_list = list()
        # if healthy_lifestyle_filter:
        #     filter_or_list.append({"key": "healthy_lifestyle", "value": employee_search_filter.healthy_lifestyle_filter})

        must_filters = list()
        if range_filter_and_list and len(range_filter_and_list) > 0:
            for filter_part in range_filter_and_list:
                str_filter = '{"range": {"##field##": {"gte": ##gte##, "lte": ##lte##}}}'
                str_filter = str_filter \
                    .replace("##field##", filter_part["key"]) \
                    .replace("##gte##", str(filter_part["gte"])) \
                    .replace("##lte##", str(filter_part["lte"]))
                must_filters.append(str_filter)

            print(','.join(must_filters))

        if term_filter_and_list and len(term_filter_and_list) > 0:
            for filter_part in term_filter_and_list:
                str_filter = '{"match": {"##key##":"##value##"}}'
                str_filter = str_filter \
                    .replace("##key##", filter_part["key"]) \
                    .replace("##value##", str(filter_part["value"]))

                must_filters.append(str_filter)

            print(','.join(must_filters))

        # should_filters = None
        # if filter_or_list and len(filter_or_list) > 0:
        #     should_filters = '{"should": [{0}]}'.format(join(filter_or_list, ','))

        if len(must_filters) > 0:
            query_body = f'{{"query": {{"bool": {settings.MUST_PLACEHOLDER} }}}}'
            must_query_body = f'{{"must": [{settings.MUST_FILTER_PLACEHOLDER}]}}'

            must_query_body = must_query_body.replace(settings.MUST_FILTER_PLACEHOLDER, ",".join(must_filters))
            query_body = query_body.replace(settings.MUST_PLACEHOLDER, must_query_body)
        else:
            # get everything
            query_body = '{"query": {"match_all":{}}'

        print(query_body)
        json_query_body = json.loads(query_body)
        result = self.es_client.search(index="_all", request_timeout=5, body=json_query_body)
        if not result:
            raise ElasticSearchFailure("results were not returned from elasticsearch.")

        logging.info(f'query hits: {result["hits"]["hits"]} ')
        return result

    def get(self, no: int):
        """
        This function will get 1 document by 'no' -> id

        returns: search result of one record
        :param no: int this is the employee number to filter by
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
