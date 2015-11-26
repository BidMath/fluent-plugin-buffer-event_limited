require 'fluent/plugin/buf_file'
require 'stringio'

module Fluent
  class EventLimitedBufferChunk < FileBufferChunk
    attr_reader :record_counter

    def self.count_events(io, separator)
      events =
        case separator
        when 'msgpack' then MessagePack::Unpacker.new(io).each
        when 'newline' then io.each($/)
        when 'tab'     then io.each("\t")
        else fail ArgumentError, "Separator '#{separator}' is not supported"
        end

      events.inject(0) { |c, _| c + 1 }
    end

    def initialize(key, path, unique_id, separator, mode = "a+", symlink_path = nil)
      super(key, path, unique_id, mode = "a+", symlink_path = nil)
      @separator = separator
      @record_counter = count_events(@file)
    end

    def <<(data)
      result = super
      @record_counter += count_events(StringIO.new(data))

      return result
    end

    private

    def count_events(io)
      self.class.count_events(io, @separator)
    end
  end

  class EventLimitedFileBuffer < FileBuffer
    Fluent::Plugin.register_buffer('event_limited', self)

    config_param :buffer_chunk_records_limit, :integer, :default => Float::INFINITY
    config_param :buffer_chunk_message_separator, :string, :default => 'msgpack'

    def storable?(chunk, data)
      event_count = chunk_class.count_events(StringIO.new(data), @buffer_chunk_message_separator)

      (chunk.record_counter + event_count <= @buffer_chunk_records_limit) &&
        ((chunk.size + data.bytesize) <= @buffer_chunk_limit)
    end

    def emit(key, data, chain)
      key = key.to_s
      flush_trigger = false

      synchronize do
        # chunk unique id is generated in #new_chunk
        chunk = (@map[key] ||= new_chunk(key))

        # If the data fits the current chunk do it quickly
        if storable?(chunk, data)
          chain.next
          chunk << data
          return (flush_trigger = false)
        end

        event_stream = unpack(data)

        # Partition the data into chunks that can be written into new chunks
        while !event_stream.empty?
          # Prepare the chunk for write
          if full?(chunk)
            @queue.synchronize do
              enqueue(chunk) # this is buffer enqueue *hook*
              flush_trigger ||= @queue.empty?
              @queue << chunk
              chunk = (@map[key] = new_chunk(key))
            end
          end

          chain.next
          data_chunk = event_stream.pop(remaining_storage(chunk))
          chunk << data_chunk.map(&method(:pack)).join
        end

        return flush_trigger
      end  # synchronize
    end

    def new_chunk(key)
      encoded_key = encode_key(key)
      path, tsuffix = make_path(encoded_key, 'b')
      unique_id = tsuffix_to_unique_id(tsuffix)

      chunk_factory(key, path, unique_id, 'a+')
    end

    # Copied here from
    # https://github.com/fluent/fluentd/blob/d3ae305b6e7521fafac6ad30c6b0a8763c363b65/lib/fluent/plugin/buf_file.rb#L128-L165
    def resume
      maps = []
      queues = []

      Dir.glob("#{@buffer_path_prefix}*#{@buffer_path_suffix}") do |path|
        identifier_part = chunk_identifier_in_path(path)
        next unless (m = PATH_MATCH.match(identifier_part))

        key = decode_key(m[1])
        bq = m[2]
        tsuffix = m[3]
        timestamp = m[3].to_i(16)
        unique_id = tsuffix_to_unique_id(tsuffix)

        case bq
        when 'b'
          maps << [timestamp, chunk_factory(key, path, unique_id, 'a+')]
        when 'q'
          queues << [timestamp, chunk_factory(key, path, unique_id, 'r')]
        end
      end

      map = {}
      maps
        .sort_by { |(timestamp, chunk)| timestamp }
        .each    { |(timestamp, chunk)| map[chunk.key] = chunk }

      queue = queues
        .sort_by { |(timestamp, _chunk)| timestamp }
        .map     { |(_timestamp, chunk)| chunk }

      return queue, map
    end

    private

    def unpack(data)
      io = StringIO.new(data)

      ( case @buffer_chunk_message_separator
        when 'msgpack' then MessagePack::Unpacker.new(io).each
        when 'newline' then io.each($/)
        end
      ).to_a
    end

    def pack(data)
      case @buffer_chunk_message_separator
      when 'msgpack' then MessagePack.pack(data)
      when 'newline' then data.join($/)
      end
    end

    def full?(chunk)
      chunk.record_counter == @buffer_chunk_records_limit
    end

    def remaining_storage(chunk)
      @buffer_chunk_records_limit - chunk.record_counter
    end

    def chunk_factory(key, path, uniq_id, mode)
      chunk_class.new(key, path, uniq_id, @buffer_chunk_message_separator, mode, @symlink_path)
    end

    def chunk_class
      EventLimitedBufferChunk
    end
  end
end
