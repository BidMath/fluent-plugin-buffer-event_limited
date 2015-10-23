require_relative '../test_helper'
require_relative 'test_event_recorder_buffered_output'
require_relative 'dummy_chain'

class EventLimitedFileBufferTest < Test::Unit::TestCase
  def setup
    @buffer = Tempfile.new('event-limited-file-buffer')
  end

  def teardown
    @buffer.close
    @buffer.unlink
  end

  def default_config
    %[
      buffer_type event_limited
      flush_interval 0.1
      try_flush_interval 0.03
      buffer_chunk_records_limit 10
      buffer_path #{@buffer.path}
    ]
  end

  def create_driver(conf = default_config, tag = 'test')
    Fluent::Test::OutputTestDriver
      .new(Fluent::TestEventRecorderBufferedOutput, tag)
      .configure(conf)
  end

  def test_plugin_configuration
    output = create_driver.instance
    buffer = output.instance_variable_get(:@buffer)

    assert output # successfully configured
    assert_equal 0.1,  output.flush_interval
    assert_equal 0.03, output.try_flush_interval
    assert_equal 10,   buffer.buffer_chunk_records_limit
  end

  def test_emit
    d = create_driver

    buffer = d.instance.instance_variable_get(:@buffer)
    assert buffer
    buffer.start

    assert_nil buffer.instance_variable_get(:@map)['']

    d.emit({"a" => 1})
    assert_equal 1, buffer.instance_variable_get(:@map)[''].record_counter

    d.emit({"a" => 2}); d.emit({"a" => 3}); d.emit({"a" => 4})
    d.emit({"a" => 5}); d.emit({"a" => 6}); d.emit({"a" => 7});
    d.emit({"a" => 8});
    assert_equal 8, buffer.instance_variable_get(:@map)[''].record_counter

    chain = DummyChain.new
    tag = d.instance.instance_variable_get(:@tag)
    time = Time.now.to_i

    # flush_trigger false
    assert !buffer.emit(tag, d.instance.format(tag, time, {"a" => 9}), chain)
    assert_equal 9, buffer.instance_variable_get(:@map)[''].record_counter

    # flush_trigger false
    assert !buffer.emit(tag, d.instance.format(tag, time, {"a" => 10}), chain)
    assert_equal 10, buffer.instance_variable_get(:@map)[''].record_counter

    # flush_trigger true
    assert buffer.emit(tag, d.instance.format(tag, time, {"a" => 11}), chain)
    assert_equal 1, buffer.instance_variable_get(:@map)[''].record_counter # new chunk

    # flush_trigger false
    assert !buffer.emit(tag, d.instance.format(tag, time, {"a" => 12}), chain)
    assert_equal 2, buffer.instance_variable_get(:@map)[''].record_counter
  end
end
