require 'fluent/plugin/buf_file'

module Fluent
  class EventLimitedBufferChunk < FileBufferChunk
    attr_reader :record_counter

    def initialize(key, path, unique_id, mode = "a+", symlink_path = nil)
      super
      @record_counter = %x{wc -l < "#{path}"}.to_i
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

    # Copied here from
    # https://github.com/fluent/fluentd/blob/d3ae305b6e7521fafac6ad30c6b0a8763c363b65/lib/fluent/plugin/buf_file.rb#L128-L165
    def resume
      maps = []
      queues = []

      Dir.glob("#{@buffer_path_prefix}*#{@buffer_path_suffix}") {|path|
        identifier_part = chunk_identifier_in_path(path)
        if m = PATH_MATCH.match(identifier_part)
          key = decode_key(m[1])
          bq = m[2]
          tsuffix = m[3]
          timestamp = m[3].to_i(16)
          unique_id = tsuffix_to_unique_id(tsuffix)

          if bq == 'b'
            chunk = EventLimitedBufferChunk.new(key, path, unique_id, "a+")
            maps << [timestamp, chunk]
          elsif bq == 'q'
            chunk = EventLimitedBufferChunk.new(key, path, unique_id, "r")
            queues << [timestamp, chunk]
          end
        end
      }

      map = {}
      maps.sort_by {|(timestamp,chunk)|
        timestamp
      }.each {|(timestamp,chunk)|
        map[chunk.key] = chunk
      }

      queue = queues.sort_by {|(timestamp,chunk)|
        timestamp
      }.map {|(timestamp,chunk)|
        chunk
      }

      return queue, map
    end

    def storable?(chunk, data)
      chunk.record_counter < @buffer_chunk_records_limit &&
        (chunk.size + data.bytesize) <= @buffer_chunk_limit
    end
  end
end
