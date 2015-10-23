require 'rubygems'
require 'bundler/setup'
require 'test/unit'

unless ENV.has_key?('VERBOSE')
  class NullLogger
    def method_missing(method, *args); end
  end
  $log = NullLogger.new
end

require 'fluent/test'
require 'fluent/plugin/buf_event_limited'
Fluent::Test.setup
