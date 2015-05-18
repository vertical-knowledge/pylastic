from elasticsearch.exceptions import AuthorizationException, NotFoundError
from pylastic import close_index, close_indices, open_index, \
    open_indices, create_index, update_alias, remove_alias, add_alias, \
    is_index_closed, wait_for_index_green
from mock import Mock, patch
from unittest import TestCase


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
        close_index(self.elastic_client, index)
        self.elastic_client.indices.close.assert_called_once_with(index=index)


    def test_raises_exception_if_no_response(self):
        self.elastic_client.indices.close.return_value = None
        self.assertRaises(Exception, close_index, self.elastic_client, 'foo')


    def test_returns_elastic_response(self):
        expected_return = 'bar'
        self.elastic_client.indices.close.return_value = expected_return
        self.assertEqual(expected_return, close_index(self.elastic_client, 'foo'))


class TestCloseIndices(PylasticTest):
    """
    Test close_indices
    """


    @patch('pylastic.helpers.close_index')
    def test_calls_close_index_for_each_index_given(self, mock_close_index):
        indices = range(5)
        failures = close_indices(self.elastic_client, indices)
        self.assertEqual(5, mock_close_index.call_count)


    def test_returns_failures_instead_of_raising_exception(self):
        self.elastic_client.indices.close.return_value = None
        index = 'foo'
        failures = close_indices(self.elastic_client, [index])
        self.assertEqual(1, len(failures))
        self.assertEqual(index, failures[0])


class TestOpenIndex(PylasticTest):
    """
    Test open_index
    """


    def test_calls_elastic_client_indices_open(self):
        index = 'foo'
        open_index(self.elastic_client, index)
        self.elastic_client.indices.open.assert_called_once_with(index=index)


    def test_raises_exception_if_no_response(self):
        self.elastic_client.indices.open.return_value = None
        self.assertRaises(Exception, open_index, self.elastic_client, 'foo')


    def test_returns_elastic_response(self):
        expected_return = 'bar'
        self.elastic_client.indices.open.return_value = expected_return
        self.assertEqual(expected_return, open_index(self.elastic_client, 'foo'))


class TestOpenIndices(PylasticTest):
    """
    Test open_indices
    """


    @patch('pylastic.helpers.open_index')
    def test_calls_open_index_for_each_index_given(self, mock_open_index):
        indices = range(5)
        failures = open_indices(self.elastic_client, indices)
        self.assertEqual(5, mock_open_index.call_count)


    def test_returns_failures_instead_of_raising_exception(self):
        self.elastic_client.indices.open.return_value = None
        index = 'foo'
        failures = open_indices(self.elastic_client, [index])
        self.assertEqual(1, len(failures))
        self.assertEqual(index, failures[0])


class TestCreateIndex(PylasticTest):
    """
    Test create_index
    """


    def test_calls_elastic_with_correct_body(self):
        create_index(self.elastic_client, 'foo', 'bar')
        self.elastic_client.indices.create.assert_called_once_with(
            index='foo', body='bar')


class TestUpdateAlias(PylasticTest):
    """
    Test update_alias
    """


    def test_calls_elastic_with_correct_body(self):
        update_alias(self.elastic_client, 'foo', 'bar', 'baz')
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
        self.assertRaises(Exception, update_alias, self.elastic_client, 'foo', 'bar', 'baz')


    def test_returns_elastic_response(self):
        expected_return = 'bar'
        self.elastic_client.indices.update_aliases.return_value = expected_return
        self.assertEqual(expected_return, update_alias(self.elastic_client, 'foo', 'bar', 'baz'))


class TestAddAlias(PylasticTest):
    """
    Test add_alias
    """


    @patch('pylastic.helpers.update_alias')
    def test_calls_update_alias_with_correct_args(self, mock_update_alias):
        add_alias(self.elastic_client, 'foo', 'bar')
        mock_update_alias.assert_called_once_with(self.elastic_client,
                                                  'foo',
                                                  'bar',
                                                  'add')


class TestRemoveAlias(PylasticTest):
    """
    Test remove_alias
    """


    @patch('pylastic.helpers.update_alias')
    def test_calls_update_alias_with_correct_args(self, mock_update_alias):
        remove_alias(self.elastic_client, 'foo', 'bar')
        mock_update_alias.assert_called_once_with(self.elastic_client,
                                                  'foo',
                                                  'bar',
                                                  'remove')


class TestIsIndexClosed(PylasticTest):
    """
    Test is_index_closed
    """


    def test_returns_true_if_index_closed(self):
        def stats(index):
            raise AuthorizationException(403, 'IndexClosedException')
        self.elastic_client.indices.stats = stats
        self.assertTrue(is_index_closed(self.elastic_client, 'foo'))


    def test_returns_false_if_index_open(self):
        self.assertFalse(is_index_closed(self.elastic_client, 'foo'))


    def test_raises_exception_if_index_does_not_exist(self):
        def stats(index):
            raise NotFoundError(404, 'IndexMissingException')
        self.elastic_client.indices.stats = stats
        self.assertRaises(NotFoundError, is_index_closed, self.elastic_client, 'foo')


class TestWaitForIndexGreen(PylasticTest):
    """
    Test wait_for_index_green
    """

    def setUp(self):
        super(TestWaitForIndexGreen, self).setUp()
        self.elastic_client.cluster.health.return_value = {'status': 'green'}


    @patch('pylastic.helpers.is_index_closed', lambda x, y: False)
    def test_returns_when_index_green(self):
        self.assertEqual(None, wait_for_index_green(self.elastic_client, 'foo'))


    @patch('pylastic.helpers.is_index_closed', lambda x, y: False)
    def test_raises_exception_if_index_does_not_exist(self):
        self.elastic_client.indices.exists.return_value = False
        self.assertRaises(Exception, wait_for_index_green, self.elastic_client, 'foo')


    @patch('pylastic.helpers.is_index_closed', lambda x, y: True)
    def test_raises_exception_if_index_is_closed(self):
        self.assertRaises(Exception, wait_for_index_green, self.elastic_client, 'foo')


    @patch('pylastic.helpers.is_index_closed', lambda x, y: False)
    def test_raises_exception_if_index_is_not_green(self):
        self.elastic_client.cluster.health.return_value = {'status': 'red'}
        self.assertRaises(Exception, wait_for_index_green, self.elastic_client, 'foo')


    @patch('pylastic.helpers.is_index_closed', lambda x, y: False)
    def test_raises_exception_if_no_response(self):
        self.elastic_client.cluster.health.return_value = None
        self.assertRaises(Exception, wait_for_index_green, self.elastic_client, 'foo')
