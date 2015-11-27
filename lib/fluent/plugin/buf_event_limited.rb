require 'fluent/plugin/buf_file'
require 'stringio'

module Fluent
  class MessagePackFormattedBufferData
    attr_reader :data

    def initialize(data)
      @data = data.to_str.freeze
    end

    def records
      @records ||= (data.empty? ? [] : unpack(data)).freeze
    end

    def as_events
      records.dup
    end

    def size
      @size ||= records.size
    end

    alias_method :to_str, :data
    alias_method :as_msg_pack, :data

    private

    def unpack(data)
      MessagePack::Unpacker.new(StringIO.new(data)).each.to_a
    end
  end

  class EventLimitedBufferChunk < FileBufferChunk
    attr_reader :record_count, :limit

    def initialize(key, path, unique_id, limit, mode = "a+")
      super(key, path, unique_id, mode = "a+")
      @limit = limit
      @record_count = MessagePackFormattedBufferData.new(read).size
    end

    def <<(data, record_count)
      super(data)
      @record_count += record_count
    end
    alias_method :write, :<<

    def remaining_capacity
      @limit - record_count
    end

    def full?
      record_count >= limit
    end
  end

  class EventLimitedFileBuffer < FileBuffer
    Fluent::Plugin.register_buffer('event_limited', self)

    config_param :buffer_chunk_records_limit, :integer, :default => Float::INFINITY

    def emit(key, data, chain)
      data = MessagePackFormattedBufferData.new(data)
      key = key.to_s
      flush_trigger = false

      synchronize do
        # Get the active chunk if it exists
        chunk = (@map[key] ||= new_chunk(key))

        # Partition the data into chunks that can be written into new chunks
        events = data.as_events
        [
          events.shift(chunk.remaining_capacity),
          *events.each_slice(@buffer_chunk_records_limit)
        ].each do |event_group|
          chunk, queue_size = rotate_chunk!(chunk, key)
          # Trigger flush only when we put the first chunk into it
          flush_trigger ||= (queue_size == 0)

          chain.next
          chunk.write(
            event_group.map { |d| MessagePack.pack(d) }.join(''),
            event_group.size
          )
        end

        return flush_trigger
      end
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

    def rotate_chunk!(chunk, key)
      queue_size = nil
      return chunk unless chunk.full?

      @queue.synchronize do
        queue_size = @queue.size
        enqueue(chunk) # this is buffer enqueue *hook*
        @queue << chunk
        chunk = (@map[key] = new_chunk(key))
      end

      return chunk, queue_size
    end

    def storable?(chunk, data)
      (chunk.record_count + data.size) <= @buffer_chunk_records_limit
    end

    def chunk_factory(key, path, uniq_id, mode)
      EventLimitedBufferChunk.new(key, path, uniq_id, @buffer_chunk_records_limit, mode)
    end
  end
end
