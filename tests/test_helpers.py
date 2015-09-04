from elasticsearch.exceptions import AuthorizationException, NotFoundError
import pylastic.helpers as helpers
from mock import Mock, patch
from unittest2 import TestCase


class PylasticTest(TestCase):
    """
    Provide a base setUp for subclass tests
    """

    def setUp(self):
        self.elastic_client = Mock()


class TestCloseIndex(PylasticTest):
    """
    Test close_index
    """

    def test_calls_elastic_client_indices_close(self):
        index = 'foo'
        helpers.close_index(self.elastic_client, index)
        self.elastic_client.indices.close.assert_called_once_with(index=index)

    def test_raises_exception_if_no_response(self):
        self.elastic_client.indices.close.return_value = None
        self.assertRaises(Exception, helpers.close_index, self.elastic_client, 'foo')

    def test_returns_elastic_response(self):
        expected_return = 'bar'
        self.elastic_client.indices.close.return_value = expected_return
        self.assertEqual(expected_return, helpers.close_index(self.elastic_client, 'foo'))


class TestCloseIndices(PylasticTest):
    """
    Test close_indices
    """

    @patch('pylastic.helpers.close_index')
    def test_calls_close_index_for_each_index_given(self, mock_close_index):
        indices = range(5)
        failures = helpers.close_indices(self.elastic_client, indices)
        self.assertEqual(5, mock_close_index.call_count)
        self.assertEqual(0, len(failures))

    def test_returns_failures_instead_of_raising_exception(self):
        self.elastic_client.indices.close.return_value = None
        index = 'foo'
        failures = helpers.close_indices(self.elastic_client, [index])
        self.assertEqual(1, len(failures))
        self.assertEqual(index, failures[0])


class TestOpenIndex(PylasticTest):
    """
    Test open_index
    """

    def test_calls_elastic_client_indices_open(self):
        index = 'foo'
        helpers.open_index(self.elastic_client, index)
        self.elastic_client.indices.open.assert_called_once_with(index=index)

    def test_raises_exception_if_no_response(self):
        self.elastic_client.indices.open.return_value = None
        self.assertRaises(Exception, helpers.open_index, self.elastic_client, 'foo')

    def test_returns_elastic_response(self):
        expected_return = 'bar'
        self.elastic_client.indices.open.return_value = expected_return
        self.assertEqual(expected_return, helpers.open_index(self.elastic_client, 'foo'))


class TestOpenIndices(PylasticTest):
    """
    Test open_indices
    """

    @patch('pylastic.helpers.open_index')
    def test_calls_open_index_for_each_index_given(self, mock_open_index):
        indices = range(5)
        failures = helpers.open_indices(self.elastic_client, indices)
        self.assertEqual(5, mock_open_index.call_count)
        self.assertEqual(0, len(failures))

    def test_returns_failures_instead_of_raising_exception(self):
        self.elastic_client.indices.open.return_value = None
        index = 'foo'
        failures = helpers.open_indices(self.elastic_client, [index])
        self.assertEqual(1, len(failures))
        self.assertEqual(index, failures[0])


class TestCreateIndex(PylasticTest):
    """
    Test create_index
    """

    def test_calls_elastic_with_correct_body(self):
        helpers.create_index(self.elastic_client, 'foo', 'bar')
        self.elastic_client.indices.create.assert_called_once_with(
            index='foo', body='bar')


class TestUpdateAlias(PylasticTest):
    """
    Test update_alias
    """

    def test_calls_elastic_with_correct_body(self):
        helpers.update_alias(self.elastic_client, 'foo', 'bar', 'baz')
        self.elastic_client.indices.update_aliases.assert_called_once_with(
            body = {
                'actions': [
                    {
                        'baz': {
                            'alias': 'foo',
                            'index': 'bar'
                        }
                    }
                ]
            }
        )

    def test_raises_exception_if_no_response(self):
        self.elastic_client.indices.update_aliases.return_value = None
        self.assertRaises(Exception, helpers.update_alias, self.elastic_client, 'foo', 'bar', 'baz')

    def test_returns_elastic_response(self):
        expected_return = 'bar'
        self.elastic_client.indices.update_aliases.return_value = expected_return
        self.assertEqual(expected_return, helpers.update_alias(self.elastic_client, 'foo', 'bar', 'baz'))


class TestAddAlias(PylasticTest):
    """
    Test add_alias
    """

    @patch('pylastic.helpers.update_alias')
    def test_calls_update_alias_with_correct_args(self, mock_update_alias):
        helpers.add_alias(self.elastic_client, 'foo', 'bar')
        mock_update_alias.assert_called_once_with(self.elastic_client,
                                                  'foo',
                                                  'bar',
                                                  'add')


class TestAddAliases(PylasticTest):
    """
    Test add_aliases
    """

    @patch('pylastic.helpers.add_alias')
    def test_calls_add_alias_for_each_index_given(self, mock_add_alias):
        indices = range(5)
        failures = helpers.add_aliases(self.elastic_client, 'foo', indices)
        self.assertEqual(5, mock_add_alias.call_count)
        self.assertEqual(0, len(failures))


    def test_returns_failures_instead_of_raising_exception(self):
        self.elastic_client.indices.update_aliases.return_value = None
        index = 'foo'
        failures = helpers.add_aliases(self.elastic_client, index, [index])
        self.assertEqual(1, len(failures))
        self.assertEqual(index, failures[0])


class TestRemoveAlias(PylasticTest):
    """
    Test remove_alias
    """

    @patch('pylastic.helpers.update_alias')
    def test_calls_update_alias_with_correct_args(self, mock_update_alias):
        helpers.remove_alias(self.elastic_client, 'foo', 'bar')
        mock_update_alias.assert_called_once_with(self.elastic_client,
                                                  'foo',
                                                  'bar',
                                                  'remove')


class TestRemoveAliases(PylasticTest):
    """
    Test remove_aliases
    """

    @patch('pylastic.helpers.remove_alias')
    def test_calls_add_alias_for_each_index_given(self, mock_remove_alias):
        indices = range(5)
        failures = helpers.remove_aliases(self.elastic_client, 'foo', indices)
        self.assertEqual(5, mock_remove_alias.call_count)
        self.assertEqual(0, len(failures))

    def test_returns_failures_instead_of_raising_exception(self):
        self.elastic_client.indices.update_aliases.return_value = None
        index = 'foo'
        failures = helpers.remove_aliases(self.elastic_client, index, [index])
        self.assertEqual(1, len(failures))
        self.assertEqual(index, failures[0])


class TestIsIndexClosed(PylasticTest):
    """
    Test is_index_closed
    """

    def test_returns_true_if_index_closed(self):
        def stats(index):
            raise AuthorizationException(403, 'IndexClosedException')
        self.elastic_client.indices.stats = stats
        self.assertTrue(helpers.is_index_closed(self.elastic_client, 'foo'))

    def test_returns_false_if_index_open(self):
        self.assertFalse(helpers.is_index_closed(self.elastic_client, 'foo'))

    def test_raises_exception_if_index_does_not_exist(self):
        def stats(index):
            raise NotFoundError(404, 'IndexMissingException')
        self.elastic_client.indices.stats = stats
        self.assertRaises(NotFoundError, helpers.is_index_closed, self.elastic_client, 'foo')


class TestWaitForIndex(PylasticTest):
    """
    Test wait_for_index
    """

    def setUp(self):
        super(TestWaitForIndex, self).setUp()
        self.elastic_client.cluster.health.return_value = {'status': 'green'}

    def test_calls_cluster_health_with_given_status(self):
        helpers.wait_for_index(self.elastic_client, 'foo', 'green')
        self.elastic_client.cluster.health.assert_called_once_with(
            index='foo',
            wait_for_status='green',
            timeout='600s')

    def test_defaults_status_to_yellow(self):
        self.elastic_client.cluster.health.return_value = {'status': 'yellow'}
        helpers.wait_for_index(self.elastic_client, 'foo')
        self.elastic_client.cluster.health.assert_called_once_with(
            index='foo',
            wait_for_status='yellow',
            timeout='600s')

    def test_calls_wait_for_status_with_correct_timeout(self):
        helpers.wait_for_index(self.elastic_client, 'foo', 'green')
        self.elastic_client.cluster.health.assert_called_once_with(
            index='foo',
            wait_for_status='green',
            timeout='600s')

    @patch('pylastic.helpers.is_index_closed', lambda x, y: False)
    def test_returns_when_index_correct_status(self):
        self.assertEqual(None, helpers.wait_for_index(self.elastic_client, 'foo', 'green'))

    @patch('pylastic.helpers.is_index_closed', lambda x, y: False)
    def test_raises_exception_if_index_does_not_exist(self):
        self.elastic_client.indices.exists.return_value = False
        self.assertRaises(Exception, helpers.wait_for_index, self.elastic_client, 'foo')
        self.elastic_client.indices.exists.assert_called_once_with(index='foo')

    @patch('pylastic.helpers.is_index_closed', lambda x, y: True)
    def test_raises_exception_if_index_is_closed(self):
        self.assertRaises(Exception, helpers.wait_for_index, self.elastic_client, 'foo', 'green')

    @patch('pylastic.helpers.is_index_closed', lambda x, y: False)
    def test_raises_exception_if_index_is_not_the_correct_status(self):
        self.elastic_client.cluster.health.return_value = {'status': 'red'}
        self.assertRaises(Exception, helpers.wait_for_index, self.elastic_client, 'foo', 'green')

    @patch('pylastic.helpers.is_index_closed', lambda x, y: False)
    def test_raises_exception_if_no_response(self):
        self.elastic_client.cluster.health.return_value = None
        self.assertRaises(Exception, helpers.wait_for_index, self.elastic_client, 'foo', 'green')


class TestWaitForIndexGreen(PylasticTest):
    """
    Test wait_for_index_green
    """

    @patch('pylastic.helpers.wait_for_index')
    def test_calls_wait_for_index_correctly(self, mock_wait_for_index):
        index = 'index'
        timeout = 'timeout'
        helpers.wait_for_index_green(self.elastic_client, index, timeout)
        mock_wait_for_index.assert_called_once_with(self.elastic_client, index, helpers.STATUS_GREEN, timeout)
