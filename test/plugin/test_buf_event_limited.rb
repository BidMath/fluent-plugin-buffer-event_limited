require_relative '../test_helper'
require 'fluent/plugin/buf_event_limited'
require_relative 'test_event_recorder_buffered_output'
require_relative 'dummy_chain'
require 'msgpack'

class Hash
  def corresponding_proxies
    @corresponding_proxies ||= []
  end

  def to_masked_element
    self
  end
end

class EventLimitedFileBufferTest < Test::Unit::TestCase
  def setup
    @buffer_path = Dir.mktmpdir('event-limited-file-buffer')
  end

  def teardown
    FileUtils.rmdir @buffer_path
  end

  def default_config
    %[
      buffer_type event_limited
      flush_interval 0.1
      try_flush_interval 0.03
      buffer_chunk_records_limit 10
      buffer_chunk_message_separator newline
      buffer_path #{@buffer_path}
    ]
  end

  def create_driver(conf = default_config, tag = 'test')
    Fluent::Test::OutputTestDriver
      .new(Fluent::TestEventRecorderBufferedOutput, tag)
      .configure(conf)
  end

  def create_buffer_with_attributes(config = {})
    config = {
      'buffer_path' => @buffer_path,
      'buffer_chunk_message_separator' => 'newline'
    }.merge(config)
    buf = Fluent::EventLimitedFileBuffer.new
    Fluent::EventLimitedFileBuffer.send(:class_variable_set, :'@@buffer_paths', {})
    buf.configure(config)
    prefix = buf.instance_eval{ @buffer_path_prefix }
    suffix = buf.instance_eval{ @buffer_path_suffix }

    [buf, prefix, suffix]
  end

  def test_plugin_configuration
    output = create_driver.instance
    buffer = output.instance_variable_get(:@buffer)

    assert output # successfully configured
    assert_equal 0.1,  output.flush_interval
    assert_equal 0.03, output.try_flush_interval
    assert_equal 10,   buffer.buffer_chunk_records_limit
    assert_equal 'newline', buffer.buffer_chunk_message_separator
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

  def test_resume_from_plain_text_chunk
    # Setup buffer to test chunks
    buf1, prefix, suffix = create_buffer_with_attributes
    buf1.start

    # Create chunks to test
    chunk1 = buf1.new_chunk('key1')
    chunk2 = buf1.new_chunk('key2')
    assert_equal 0, chunk1.record_counter
    assert_equal 0, chunk2.record_counter

    # Write data into chunks
    chunk1 << "data1\ndata2\n"
    chunk2 << "data3\ndata4\n"

    # Enqueue chunk1 and leave chunk2 open
    buf1.enqueue(chunk1)
    assert \
      chunk1.path =~ /\A#{prefix}[-_.a-zA-Z0-9\%]+\.q[0-9a-f]+#{suffix}\Z/,
      "chunk1 must be enqueued"
    assert \
      chunk2.path =~ /\A#{prefix}[-_.a-zA-Z0-9\%]+\.b[0-9a-f]+#{suffix}\Z/,
      "chunk2 is not enqueued yet"
    buf1.shutdown

    # Setup a new buffer to test resume
    buf2, *_ = create_buffer_with_attributes
    queue, map = buf2.resume

    # Returns with the open and the closed buffers
    assert_equal 1, queue.size # closed buffer
    assert_equal 1, map.values.size # open buffer

    # The paths of the resumed chunks are the same but they themselfs are not
    resumed_chunk1 = queue.first
    resumed_chunk2 = map.values.first
    assert_equal chunk1.path, resumed_chunk1.path
    assert_equal chunk2.path, resumed_chunk2.path
    assert chunk1 != resumed_chunk1
    assert chunk2 != resumed_chunk2

    # Resume with the proper type of buffer chunk
    assert_equal Fluent::EventLimitedBufferChunk, resumed_chunk1.class
    assert_equal Fluent::EventLimitedBufferChunk, resumed_chunk2.class

    assert_equal "data1\ndata2\n", resumed_chunk1.read
    assert_equal "data3\ndata4\n", resumed_chunk2.read

    assert_equal 2, resumed_chunk1.record_counter
    assert_equal 2, resumed_chunk2.record_counter
  end

  def test_resume_from_msgpack_chunks
    # Setup buffer to test chunks
    buf1, prefix, suffix = create_buffer_with_attributes({'buffer_chunk_message_separator' => 'msgpack'})
    buf1.start

    # Create chunks to test
    chunk1 = buf1.new_chunk('key1')
    chunk2 = buf1.new_chunk('key2')
    assert_equal 0, chunk1.record_counter
    assert_equal 0, chunk2.record_counter

    # Write data into chunks
    chunk1 << MessagePack.pack('data1')
    chunk1 << MessagePack.pack('data2')
    chunk2 << MessagePack.pack('data3')
    chunk2 << MessagePack.pack('data4')

    # Enqueue chunk1 and leave chunk2 open
    buf1.enqueue(chunk1)
    assert \
      chunk1.path =~ /\A#{prefix}[-_.a-zA-Z0-9\%]+\.q[0-9a-f]+#{suffix}\Z/,
      "chunk1 must be enqueued"
    assert \
      chunk2.path =~ /\A#{prefix}[-_.a-zA-Z0-9\%]+\.b[0-9a-f]+#{suffix}\Z/,
      "chunk2 is not enqueued yet"
    buf1.shutdown

    # Setup a new buffer to test resume
    buf2, *_ = create_buffer_with_attributes({'buffer_chunk_message_separator' => 'msgpack'})
    queue, map = buf2.resume

    # Returns with the open and the closed buffers
    assert_equal 1, queue.size # closed buffer
    assert_equal 1, map.values.size # open buffer

    # The paths of the resumed chunks are the same but they themselfs are not
    resumed_chunk1 = queue.first
    resumed_chunk2 = map.values.first
    assert_equal chunk1.path, resumed_chunk1.path
    assert_equal chunk2.path, resumed_chunk2.path
    assert chunk1 != resumed_chunk1
    assert chunk2 != resumed_chunk2

    # Resume with the proper type of buffer chunk
    assert_equal Fluent::EventLimitedBufferChunk, resumed_chunk1.class
    assert_equal Fluent::EventLimitedBufferChunk, resumed_chunk2.class

    assert_equal \
      MessagePack.pack('data1') + MessagePack.pack('data2'),
      resumed_chunk1.read
    assert_equal \
      MessagePack.pack('data3') + MessagePack.pack('data4'),
      resumed_chunk2.read

    assert_equal 2, resumed_chunk1.record_counter
    assert_equal 2, resumed_chunk2.record_counter
  end
end
