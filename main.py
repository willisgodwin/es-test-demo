import logging
import os

from schema.model import EmployeeSearchFilter
from util.es_data_access import ElasticSearchDataAccess, ElasticSearchManagement
from util.es_mappings import es_test_mapping

logger = logging.getLogger(__name__)


def create_index_and_populate():
    """
    This function will delete/create and load the index with test data
    """
    mgmt = ElasticSearchManagement()
    data_file_path = os.path.join("data", "employee.csv")
    logging.info(f"data_file_path:'{data_file_path}'")
    if mgmt.create_index(mapping=es_test_mapping):
        is_success = mgmt.populate_index(path=data_file_path)

        if is_success:
            print("Index created and data has been indexed successfully!")


def execute_searches():
    """
    This function will execute a few test searches via code.

    20221118 when you come back plumb the "should" filters WHG
    """
    search_client = ElasticSearchDataAccess()
    number_filter = 4
    search_one_result = search_client.get(no=number_filter)
    logging.info(search_one_result)

    search_filter = EmployeeSearchFilter(occupation_filter="software engineer")
    search_many_results = search_client.search(search_filter)
    logging.info(search_many_results)

    search_filter = EmployeeSearchFilter(age_filter=34, gender_filter=0)
    search_many_results = search_client.search(search_filter)
    logging.info(search_many_results)

    search_filter = EmployeeSearchFilter(age_filter=34, salary_range_filter_gte=28000, salary_range_filter_lte=38000)
    search_many_results = search_client.search(search_filter)
    logging.info(search_many_results)


if __name__ == '__main__':
    logging.basicConfig(filename="es.log",
                        level=logging.INFO,
                        format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
                        datefmt='%H:%M:%S')
    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

    create_index_and_populate()
    execute_searches()
