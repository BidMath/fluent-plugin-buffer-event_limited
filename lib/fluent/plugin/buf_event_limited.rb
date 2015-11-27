require 'fluent/plugin/buf_file'
require 'stringio'

module Fluent
  class MessagePackFormattedBufferData
    attr_reader :data

    def initialize(data)
      @data = data.to_str.freeze
    end

    # Partition the data into required sizes
    def each_slice(target_sizes)
      target_size = target_sizes.next
      slice_size = 0
      slice_data = ''

      reader.each do |event|
        if slice_size == target_size
          yield(slice_data, slice_size)

          target_size = target_sizes.next
          slice_size = 0
          slice_data = ''
        end

        slice_data << pack(event)
        slice_size += 1
      end

      yield(slice_data, slice_size)
    end

    def size
      @size ||= reader.each.reduce(0) { |c, _| c + 1 }
    end

    alias_method :to_str, :data
    alias_method :as_msg_pack, :data

    private

    def pack(event)
      MessagePack.pack(event)
    end

    def reader
      @reader ||= MessagePack::Unpacker.new(StringIO.new(data))
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

    def emit(key, data, chain)
      data = MessagePackFormattedBufferData.new(data)
      key = key.to_s
      flush_trigger = false

      synchronize do
        # Get the current open chunk
        chunk = (@map[key] ||= new_chunk(key))

        data.each_slice(chunk_sizes(chunk.remaining_capacity)) do |data, size|
          chain.next
          chunk.write(data, size)
          chunk, queue_size = rotate_chunk!(chunk, key)
          flush_trigger ||= (queue_size == 0)
        end
      end

      return flush_trigger
    end

    def new_chunk(key)
      encoded_key = encode_key(key)
      path, tsuffix = make_path(encoded_key, 'b')
      unique_id = tsuffix_to_unique_id(tsuffix)

      chunk_factory(key, path, unique_id, 'a+')
    end

    private

    def chunk_factory(key, path, uniq_id, mode)
      EventLimitedBufferChunk.new(key, path, uniq_id, @buffer_chunk_records_limit, mode)
    end

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

    # Generates infinite sequence with and initial value followed by the chunk
    # limit
    #
    # Eg.: [2, 5, 5, 5, 5, 5, ...]
    def chunk_sizes(initial_size)
      Enumerator.new do |y|
        y << initial_size
        y << @buffer_chunk_records_limit while true
      end
    end
  end
end
