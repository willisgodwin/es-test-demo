import logging
import os

from util.es_data_access import ElasticSearchDataAccess, ElasticSearchManagement
from util.es_mappings import es_test_mapping


def ping_elastic_search():
    mgmt = ElasticSearchManagement()
    data_file_path = os.path.join("data", "employee.csv")
    logging.info(f"data_file_path:'{data_file_path}'")
    if mgmt.create_index(mapping=es_test_mapping):
        mgmt.populate_index(path=data_file_path)


if __name__ == '__main__':
    ping_elastic_search()
