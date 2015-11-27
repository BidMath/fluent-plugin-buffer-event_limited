# fluent-plugin-buffer-event_limited

This gem is a mutation of the [fluent-plugin-buffer-lightening](https://github.com/tagomoris/fluent-plugin-buffer-lightening) buffer plugin by [tagomoris](https://github.com/tagomoris).

* The buffer is able to limit the number of events that are buffered in a buffer chunk.
* The buffer only supports output plugins that return msgpack from their `#format` methods.
* The buffer doesn't check the bytesize of the buffers just the number of messages

## Installation

Do `gem install fluent-plugin-buffer-event_limited` or `fluent-gem ...`.

## Configuration

EventLimited buffer plugin can be enabled with all of buffered output plugins.

To flush chunks per 100 records, configure like this:

```
<match data.**>
  type any_buffered_output_plugin
  buffer_type event_limited
  buffer_chunk_records_limit 100
  # other options...
</match>
```

Options of `buffer_type file` are also available:
```
<match data.**>
  type any_buffered_output_plugin
  buffer_type event_limited
  buffer_chunk_limit 10M
  buffer_chunk_records_limit 100
  # other options...
</match>
```

### For less delay

For more frequently flushing, use `flush_interval` and `try_flush_interval` with floating point values on Fluentd v0.10.42 or later:
```
<match data.**>
  type any_buffered_output_plugin
  buffer_type event_limited
  buffer_chunk_records_limit 100
  # other options...
  flush_interval 0.5
  try_flush_interval 0.1 # 0.6sec delay for worst case
</match>
```

