module Fluent
  class TestEventRecorderBufferedOutput < BufferedOutput
    Fluent::Plugin.register_output('test_event_recorder', self)

    attr_reader :written

    def start
      super
      @written = []
    end

    def format(tag, time, record)
      [tag, time, record.merge({'format_time' => Time.now.to_f})].to_msgpack
    end

    def write(chunk)
      chunk.msgpack_each do |(tag, time, record)|
        @written.push([tag, time, record.merge({'write_time' => Time.now.to_f})])
      end

      true
    end
  end
end
