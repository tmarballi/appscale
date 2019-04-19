#!/usr/bin/ruby -w

# To support the Google App Engine Datastore API in a way that is
# database-agnostic, App Engine applications store and retrieve data
# via the DatastoreServer. The server inherits this name from the storage
# format of requests in the Datastore API: Datastore Buffers.
module DatastoreServer
  # The port that we should run nginx on, to load balance requests to the
  # various DatastoreServers running on this node.
  PROXY_PORT = 8888

  # The name that nginx should use as the identifier for the DatastoreServer when it
  # we write its configuration files.
  NAME = 'appscale-datastore_server'.freeze

  # If we fail to get the number of processors we set our default number of
  # datastore servers to this value.
  DEFAULT_NUM_SERVERS = 3

  # Maximum number of concurrent requests that can be served
  # by instance of datastore
  MAXCONN = 2

  # Datastore server processes to core multiplier.
  MULTIPLIER = 2
end
