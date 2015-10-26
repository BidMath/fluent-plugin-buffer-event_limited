require 'fluent/plugin/buf_file'

module Fluent
  class EventLimitedBufferChunk < FileBufferChunk
    attr_reader :record_counter

    def initialize(key, path, unique_id, mode = "a+", symlink_path = nil)
      super
      @record_counter = 0
    end

    def <<(data)
      result = super
      @record_counter += 1

      return result
    end
  end

  class EventLimitedFileBuffer < FileBuffer
    Fluent::Plugin.register_buffer('event_limited', self)

    config_param :buffer_chunk_records_limit, :integer, :default => Float::INFINITY

    def new_chunk(key)
      encoded_key = encode_key(key)
      path, tsuffix = make_path(encoded_key, 'b')
      unique_id = tsuffix_to_unique_id(tsuffix)

      EventLimitedBufferChunk.new(key, path, unique_id, 'a+', @symlink_path)
    end

    def storable?(chunk, data)
      chunk.record_counter < @buffer_chunk_records_limit &&
        (chunk.size + data.bytesize) <= @buffer_chunk_limit
    end
  end
end
