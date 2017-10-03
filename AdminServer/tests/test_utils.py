import unittest

import re
from mock import call, patch
from mock.mock import MagicMock
from tornado import testing, gen

from appscale.admin import utils
from appscale.taskqueue.constants import InvalidQueueConfiguration


class TestUtils(unittest.TestCase):
  def test_apply_mask_to_version(self):
    given_version = {'runtime': 'python27',
                     'appscaleExtensions': {'httpPort': 80}}
    desired_fields = ['appscaleExtensions.httpPort']
    self.assertDictEqual(
      utils.apply_mask_to_version(given_version, desired_fields),
      {'appscaleExtensions': {'httpPort': 80}})

    given_version = {'appscaleExtensions': {'httpPort': 80, 'httpsPort': 443}}
    desired_fields = ['appscaleExtensions.httpPort',
                      'appscaleExtensions.httpsPort']
    self.assertDictEqual(
      utils.apply_mask_to_version(given_version, desired_fields),
      {'appscaleExtensions': {'httpPort': 80, 'httpsPort': 443}})

    given_version = {'runtime': 'python27'}
    desired_fields = ['appscaleExtensions.httpPort',
                      'appscaleExtensions.httpsPort']
    self.assertDictEqual(
      utils.apply_mask_to_version(given_version, desired_fields), {})

    given_version = {'runtime': 'python27',
                     'appscaleExtensions': {'httpPort': 80, 'httpsPort': 443}}
    desired_fields = ['appscaleExtensions']
    self.assertDictEqual(
      utils.apply_mask_to_version(given_version, desired_fields),
      {'appscaleExtensions': {'httpPort': 80, 'httpsPort': 443}})

  def test_validate_queue(self):
    valid_queues = [
      {'name': 'queue-1', 'rate': '5/s'},
      {'name': 'fooqueue', 'rate': '1/s',
       'retry_parameters': {'task_retry_limit': 7, 'task_age_limit': '2d'}},
      {'name': 'fooqueue', 'mode': 'pull'}
    ]
    invalid_queues = [
      {'name': 'a' * 101, 'rate': '5/s'},  # Name is too long.
      {'name': '*', 'rate': '5/s'},  # Invalid characters in name.
      {'name': 'fooqueue', 'rate': '5/y'},  # Invalid unit of time.
      {'name': 'fooqueue'},  # Push queues must specify rate.
      {'name': 'fooqueue', 'mode': 'pull',
       'retry_parameters': {'task_retry_limit': 'a'}}  # Invalid retry value.
    ]
    for queue in valid_queues:
      utils.validate_queue(queue)

    for queue in invalid_queues:
      self.assertRaises(InvalidQueueConfiguration, utils.validate_queue, queue)


class TestRetryCoroutine(testing.AsyncTestCase):

  @patch.object(utils.gen, 'sleep')
  @patch.object(utils.logger, 'error')
  @testing.gen_test
  def test_no_errors(self, logger_mock, gen_sleep_mock):
    gen_sleep_mock.side_effect = testing.gen.coroutine(lambda sec: sec)

    # Call dummy lambda persistently.
    persistent_work = utils.retry_coroutine(lambda: "No Errors")
    result = yield persistent_work()

    # Assert outcomes.
    self.assertEqual(result, "No Errors")
    self.assertEqual(gen_sleep_mock.call_args_list, [])
    self.assertEqual(logger_mock.call_args_list, [])

  @patch.object(utils.gen, 'sleep')
  @patch.object(utils.logger, 'error')
  @patch.object(utils.random, 'random')
  @testing.gen_test
  def test_backoff_and_logging(self, gauss_mock, logger_mock, gen_sleep_mock):
    random_value = 0.84
    gauss_mock.return_value = random_value
    gen_sleep_mock.side_effect = testing.gen.coroutine(lambda sec: sec)

    def do_work():
      raise ValueError(u"Error \u26a0!")

    persistent_work = utils.retry_coroutine(
      do_work, backoff_base=3, backoff_multiplier=0.1,
      backoff_threshold=2, max_retries=4
    )
    try:
      yield persistent_work()
      self.fail("Exception was expected")
    except ValueError:
      pass

    # Check backoff sleep calls (0.1 * (3 ** attempt) * random_value).
    sleep_args = [args[0] for args, kwargs in gen_sleep_mock.call_args_list]
    self.assertAlmostEqual(sleep_args[0], 0.33, 2)
    self.assertAlmostEqual(sleep_args[1], 0.99, 2)
    self.assertAlmostEqual(sleep_args[2], 2.2, 2)
    self.assertAlmostEqual(sleep_args[3], 2.2, 2)

    # Verify logged errors.
    expected_logs = [
      (re.compile(r"ValueError: Error \\u26a0!\nRetry #1 in 0\.3s")),
      (re.compile(r"ValueError: Error \\u26a0!\nRetry #2 in 1\.0s")),
      (re.compile(r"ValueError: Error \\u26a0!\nRetry #3 in 2\.2s")),
      (re.compile(r"ValueError: Error \\u26a0!\nRetry #4 in 2\.2s")),
    ]
    self.assertEqual(len(expected_logs), len(logger_mock.call_args_list))
    expected_messages = iter(expected_logs)
    for call_args_kwargs in logger_mock.call_args_list:
      error_message = expected_messages.next()
      self.assertRegexpMatches(call_args_kwargs[0][0], error_message)

  @patch.object(utils.gen, 'sleep')
  @testing.gen_test
  def test_exception_filter(self, gen_sleep_mock):
    gen_sleep_mock.side_effect = testing.gen.coroutine(lambda sec: sec)

    def func(exc_class, msg, retries_to_success):
      retries_to_success['counter'] -= 1
      if retries_to_success['counter'] <= 0:
        return "Succeeded"
      raise exc_class(msg)

    def err_filter(exception):
      return isinstance(exception, ValueError)

    wrapped = utils.retry_coroutine(func, retry_on_exception=err_filter)

    # Test retry helps.
    result = yield wrapped(ValueError, "Matched", {"counter": 3})
    self.assertEqual(result, "Succeeded")

    # Test retry not applicable.
    try:
      yield wrapped(TypeError, "Failed", {"counter": 3})
      self.fail("Exception was expected")
    except TypeError:
      pass

  @patch.object(utils.gen, 'sleep')
  @testing.gen_test
  def test_refresh_args(self, gen_sleep_mock):
    gen_sleep_mock.side_effect = testing.gen.coroutine(lambda sec: sec)

    def func(first, second):
      if first <= second:
        raise ValueError("First should be greater than second")
      return first, second

    # Mock will be tracking function calls
    func = MagicMock(side_effect=func)

    fresh_args_kwargs = [
      ((1,), {'second': 4}),   # causes ValueError: 1 <= 4
      ((2,), {'second': 3}),   # causes ValueError: 1 <= 4
      ((3,), {'second': 2}),   # ok: 3 > 2 (successful return here)
      ((4,), {'second': 1}),   # ok: 4 > 1 (shouldn't be used)
    ]
    refresh_func = iter(fresh_args_kwargs).next
    wrapped = utils.retry_coroutine(func, refresh_args_kwargs_func=refresh_func)

    result = yield wrapped(0, 5)  # First try should fail (0 <= 5)
    self.assertEqual(result, (3, 2))
    self.assertEqual(func.call_args_list, [
      call(0, 5),
      call(1, second=4),
      call(2, second=3),
      call(3, second=2)
    ])

  @patch.object(utils.gen, 'sleep')
  @testing.gen_test
  def test_wrapping_coroutine(self, gen_sleep_mock):
    gen_sleep_mock.side_effect = testing.gen.coroutine(lambda sec: sec)

    @gen.coroutine
    def func(first, second):
      if first <= second:
        raise ValueError("First should be greater than second")
      raise gen.Return((first, second))

    fresh_args_kwargs = [
      ((1,), {'second': 4}),   # causes ValueError: 1 <= 4
      ((3,), {'second': 2}),   # ok: 3 > 2 (successful return here)
    ]
    refresh_func = iter(fresh_args_kwargs).next
    wrapped = utils.retry_coroutine(func, refresh_args_kwargs_func=refresh_func)

    # Test retry helps.
    result = yield wrapped(0, 5)
    self.assertEqual(result, (3, 2))
