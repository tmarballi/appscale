""" Fetches and prepares the source code for revisions. """

import errno
import logging
import os
import random
import shutil

from kazoo.exceptions import NodeExistsError
from tornado import gen
from tornado.options import options

from appscale.common.appscale_utils import get_md5
from appscale.common.appscale_info import get_secret
from appscale.common.constants import VERSION_PATH_SEPARATOR
from .utils import fetch_file
from ..constants import (
  DASHBOARD_APP_ID,
  InvalidSource,
  SOURCES_DIRECTORY,
  UNPACK_ROOT
)
from ..utils import extract_source

logger = logging.getLogger('appscale-admin')


class SourceManager(object):
  """ Fetches and prepares the source code for revisions. """
  def __init__(self, zk_client, thread_pool):
    """ Creates a new SourceManager.

    Args:
      zk_client: A KazooClient.
      thread_pool: A ThreadPoolExecutor.
    """
    self.zk_client = zk_client
    self.thread_pool = thread_pool
    self.source_futures = {}

  @gen.coroutine
  def fetch_archive(self, revision_key, source_location, try_existing=True):
    """ Copies the source archive from a machine that has it.

    Args:
      revision_key: A string specifying a revision key.
      source_location: A string specifying the location of the version's
        source archive.
      try_existing: A boolean specifying that the local file system should be
        searched before copying from another machine.
    Returns:
      A string specifying the source archive's MD5 hex digest.
    """
    hosts_with_archive = yield self.thread_pool.submit(
      self.zk_client.get_children, '/apps/{}'.format(revision_key))
    assert hosts_with_archive, '{} has no hosters'.format(revision_key)

    host = random.choice(hosts_with_archive)
    host_node = '/apps/{}/{}'.format(revision_key, host)
    original_md5, _ = yield self.thread_pool.submit(
      self.zk_client.get, host_node)

    if try_existing and os.path.isfile(source_location):
      md5 = yield self.thread_pool.submit(get_md5, source_location)
      if md5 == original_md5:
        raise gen.Return(md5)

      raise InvalidSource('Source MD5 does not match')

    yield self.thread_pool.submit(fetch_file, host, source_location)
    md5 = yield self.thread_pool.submit(get_md5, source_location)
    if md5 != original_md5:
      raise InvalidSource('Source MD5 does not match')

    raise gen.Return(md5)

  @gen.coroutine
  def prepare_source(self, revision_key, location, runtime):
    """ Fetches and extracts the source code for a revision.

    Args:
      revision_key: A string specifying a revision key.
      location: A string specifying the location of the revision's source
        archive.
      runtime: A string specifying the revision's runtime.
    """
    try:
      md5 = yield self.fetch_archive(revision_key, location)
    except InvalidSource:
      md5 = yield self.fetch_archive(
        revision_key, location, try_existing=False)

    # Register as a hoster.
    new_hoster_node = '/apps/{}/{}'.format(revision_key, options.private_ip)
    try:
      yield self.thread_pool.submit(self.zk_client.create, new_hoster_node,
                                    md5, makepath=True)
    except NodeExistsError:
      logger.debug('{} is already a hoster'.format(options.private_ip))

    yield self.thread_pool.submit(extract_source, revision_key, location,
                                  runtime)

    project_id = revision_key.split(VERSION_PATH_SEPARATOR)[0]
    if project_id == DASHBOARD_APP_ID:
      self.update_secret(revision_key)

  @gen.coroutine
  def ensure_source(self, revision_key, location, runtime):
    """ Wait until the revision source is ready.

    If this method has been previously called for the same revision, it waits
    for the same future. This prevents the same archive from being fetched
    and extracted multiple times.

    Args:
      revision_key: A string specifying the revision key.
      location: A string specifying the location of the source archive.
      runtime: A string specifying the revision's runtime.
    """
    future = self.source_futures.get(revision_key)
    if future is None or (future.done() and future.exception() is not None):
      future = self.prepare_source(revision_key, location, runtime)
      self.source_futures[revision_key] = future

    yield future

  def clean_old_revisions(self, active_revisions):
    """ Cleans up the source code for old revisions.

    Args:
      active_revisions: A set of strings specifying active revision keys.
    """
    # Remove unneeded source directories.
    for source_directory in os.listdir(UNPACK_ROOT):
      if source_directory not in active_revisions:
        shutil.rmtree(os.path.join(UNPACK_ROOT, source_directory),
                      ignore_errors=True)

    # Remove obsolete archives.
    futures_to_clear = []
    for revision_key, future in self.source_futures.items():
      if not future.done():
        continue

      if revision_key in active_revisions:
        continue

      archive_location = os.path.join(SOURCES_DIRECTORY,
                                      '{}.tar.gz'.format(revision_key))
      try:
        os.remove(archive_location)
      except OSError as error:
        if error.errno != errno.ENOENT:
          raise

        logger.debug(
          '{} did not exist when trying to remove it'.format(archive_location))

      futures_to_clear.append(revision_key)

    for revision_key in futures_to_clear:
      del self.source_futures[revision_key]

  @staticmethod
  def update_secret(revision_key):
    """ Ensures the revision's secret matches the deployment secret. """
    deployment_secret = get_secret()
    revision_base = os.path.join(UNPACK_ROOT, revision_key)
    secret_module = os.path.join(revision_base, 'app', 'lib', 'secret_key.py')
    with open(secret_module) as secret_file:
      revision_secret = secret_file.read().split()[-1][1:-1]
      if revision_secret == deployment_secret:
        return

    with open(secret_module, 'w') as secret_file:
      secret_file.write("GLOBAL_SECRET_KEY = '{}'".format(deployment_secret))
