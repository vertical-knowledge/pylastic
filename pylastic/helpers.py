"""
Functions which wrap the Elastic client for convenience
"""


from elasticsearch.exceptions import AuthorizationException
from patched_elastic import scan, bulk


STATUS_GREEN = 'green'

def close_index(elastic_client, index):
    """
    Given an Elastic client and and index, attempt to close the index
    :param elastic_client: Elastic client
    :param index: Index which is to be closed
    :return: Response from Elastic
    """
    response = elastic_client.indices.close(index=index)
    if response is None:
        raise Exception('Problem occurred closing index. Index: {}'.format(index))
    return response


def close_indices(elastic_client, indices):
    """
    Attempt to close each Elastic index in a list of indices
    :param elastic_client: Elastic client
    :param indices: List of indices to close
    :return: List of failed indices
    """
    close_index_failures = []
    for index in indices:
        try:
            close_index(elastic_client, index)
        except:
            close_index_failures.append(index)
    return close_index_failures


def open_index(elastic_client, index):
    """
    Given an Elastic client and and index, attempt to open the index
    :param elastic_client: Elastic client
    :param index: Index which is to be opened
    :return: Response from Elastic
    """
    response = elastic_client.indices.open(index=index)
    if response is None:
        raise Exception('Problem occurred opening index. Index: {}'.format(index))
    return response


def open_indices(elastic_client, indices):
    """
    Attempt to open each Elastic index in a list of indices
    :param elastic_client: Elastic client
    :param indices: List of indices to open
    :return: List of failed indices
    """
    open_index_failures = []
    for index in indices:
        try:
            open_index(elastic_client, index)
        except:
            open_index_failures.append(index)
    return open_index_failures


def create_index(elastic_client, index, body):
    """
    Create a new index in Elastic with the provided settings and mapping
    :param elastic_client: Elastic client
    :param index: Index which is to be created
    :param body: Mapping and settings for the new index
    :return: Response from Elastic
    """

    response = elastic_client.indices.create(index=index, body=body)
    if response is None:
        raise Exception('Problem occurred creating index. Index: {}'.format(index))
    return response


def update_alias(elastic_client, alias, index, action):
    """
    Update an alias in Elastic, either adding or removing an index
    :param elastic_client: Elastic client
    :param alias: Alias on which to operate
    :param index: Index with which to operate
    :param action: "add" or "remove"
    :return: Response from Elastic
    """
    body = {
        'actions': [
            {
                action: {
                    'alias': alias,
                    'index': index
                }
            }
        ]
    }
    response = elastic_client.indices.update_aliases(body=body)
    if response is None:
        raise Exception('Problem occurred updating alias. Alias: {}. Index: {}'.format(alias, index))
    return response


def add_alias(elastic_client, alias, index):
    """
    Add am Elastic index to an Elastic alias
    :param elastic_client: Elastic client
    :param alias: Alias to which the index will be added
    :param index: Index to add
    :return: Response from Elastic
    """
    return update_alias(elastic_client, alias, index, 'add')


def add_aliases(elastic_client, alias, indices):
    """
    Add multiple indices to a given alias
    :param elastic_client: Elastic client
    :param alias: Alias to which indices are to be added
    :param indices: List of indices to add
    :return: List of failed indices
    """
    failures = []
    for index in indices:
        try:
            add_alias(elastic_client, alias, index)
        except:
            failures.append(index)
    return failures


def remove_alias(elastic_client, alias, index):
    """
    Remove an Elastic index from an Elastic alias
    :param elastic_client: Elastic client
    :param alias: Alias from which index is to be removed
    :param index: Index to remove
    :return: Response from Elastic
    """
    return update_alias(elastic_client, alias, index, 'remove')


def remove_aliases(elastic_client, alias, indices):
    """
    Remove multiple indices from a given alias
    :param elastic_client: Elastic client
    :param alias: Alias from which indices are to be removed
    :param indices: List of indices to remove
    :return: List of failed indices
    """
    failures = []
    for index in indices:
        try:
            remove_alias(elastic_client, alias, index)
        except:
            failures.append(index)
    return failures


def is_index_closed(elastic_client, index):
    """
    Check if an Elastic index is closed
    :param elastic_client: Elastic client
    :param index: Index to check
    :return: Boolean
    """
    try:
        elastic_client.indices.stats(index)
    except AuthorizationException as e:
        if e.status_code == 403 and 'IndexClosedException' in e.error:
            return True

    return False


def wait_for_index_green(elastic_client, index, timeout=600):
    """
    Busy wait until the given index is green status
    :param elastic_client: Elastic client
    :param index: Index to check status on
    :param timeout: Int number of seconds to wait_for_status
    :return: None
    """
    if not elastic_client.indices.exists(index=index):
        raise Exception('Index does not exist')

    if is_index_closed(elastic_client, index):
        raise Exception('Index is closed')

    response = elastic_client.cluster.health(index=index,
                                             wait_for_status=STATUS_GREEN,
                                             timeout='{}s'.format(timeout))

    if not response or not (response.get('status', '') == STATUS_GREEN):
        raise Exception('Index is not {}'.format(STATUS_GREEN))
