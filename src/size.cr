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

  struct WithUnit
    include JSON::Serializable

    getter magnitude : Int64

    def self.new(json : JSON::PullParser)
      string = json.read_string
      if match = string.match(/\A(\d+)([A-Za-z]+)\z/)
        new(match[1].to_i64, Unit.parse(match[2]))
      else
        raise ::JSON::SerializableError.new("Cannot parse a #{name} from #{string.inspect}", to_s, nil, *json.location, nil)
      end
    end

    def self.new(magnitude, unit : Unit)
      magnitude = magnitude.to_i64
      case unit
      in .b?
        new magnitude
      in .kb?
        new magnitude * 1024
      in .mb?
        new magnitude * 1024, :kb
      in .gb?
        new magnitude * 1024, :mb
      in .tb?
        new magnitude * 1024, :gb
      in .pb?
        new magnitude * 1024, :tb
      in .eb?
        new magnitude * 1024, :pb
      end
    end

    # :nodoc:
    def initialize(@magnitude)
    end

    def to_json(json : JSON::Builder) : Nil
      case magnitude
      when 0i64...2i64**10
        json.string "#{magnitude}b"
      when 2i64**10...2i64**20
        json.string "#{magnitude // 2**10}kb"
      when 2i64**20...2i64**30
        json.string "#{magnitude // 2**20}mb"
      when 2i64**30...2i64**40
        json.string "#{magnitude // 2**30}gb"
      when 2i64**40...2i64**50
        json.string "#{magnitude // 2**40}tb"
      when 2i64**50...2i64**60
        json.string "#{magnitude // 2**50}pb"
      when 2i64**60...
        json.string "#{magnitude // 2**60}eb"
      end
    end

    enum Unit
      B
      KB
      MB
      GB
      TB
      PB
      EB
    end
  end
end
