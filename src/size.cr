require "./es"
require "json"

# Represents a number of bytes, primarily used in inspecting index stats to
# provide byte eizes that are human-readable.
#
# ```
# es.indices.stats("*").all.total.store.size_in_bytes.inspect # => "5.05GB"
# ```
struct ES::Size
  @bytes : Int64

  def self.new(json : JSON::PullParser)
    new json.read_int
  end

  def initialize(@bytes)
  end

  def inspect(io) : Nil
    @bytes.humanize_bytes io, format: :jedec
  end

  def to_json(json : JSON::Builder)
    json.number @bytes
  end

  def to_i
    to_i64
  end

  def to_i64
    @bytes
  end
end
