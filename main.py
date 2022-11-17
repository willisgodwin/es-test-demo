import logging
import os

from util.es_data_access import ElasticSearchDataAccess, ElasticSearchManagement
from util.es_mappings import es_test_mapping


logger = logging.getLogger(__name__)


def ping_elastic_search():
    mgmt = ElasticSearchManagement()
    data_file_path = os.path.join("data", "employee.csv")
    logging.info(f"data_file_path:'{data_file_path}'")
    if mgmt.create_index(mapping=es_test_mapping):
        mgmt.populate_index(path=data_file_path)


if __name__ == '__main__':
    logging.basicConfig(filename="es.log", level=logging.INFO)
    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

    ping_elastic_search()
