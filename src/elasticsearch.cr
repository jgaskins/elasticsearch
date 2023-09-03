require "json"

require "./client"
require "./indices"
require "./data_streams"
require "./mappings"
require "./json_conversion"
require "./serializable"

module Elasticsearch
  struct SearchResult(T)
    include JSON::Serializable

    struct Hit(T)
      include JSON::Serializable

      @[JSON::Field(key: "_index")]
      getter index : String

      @[JSON::Field(key: "_id")]
      getter id : String

      @[JSON::Field(key: "_score")]
      getter score : Float64?

      @[JSON::Field(key: "_source")]
      getter source : T

      getter sort : Array(Float64)?
    end

    include Enumerable(Hit(T))

    def each
      hits.hits.each do |hit|
        yield hit
      end
    end

    struct Hits(T)
      include JSON::Serializable

      getter total : Totals
      getter max_score : Float64?
      getter hits : Array(Hit(T))
    end

    struct Totals
      include JSON::Serializable

      getter value : Int64
      getter relation : String
    end

    @[JSON::Field(converter: ::Elasticsearch::MillisecondsTimeSpan)]
    getter took : Time::Span
    getter timed_out : Bool
    @[JSON::Field(key: "_shards")]
    getter shards : Shards
    getter hits : Hits(T)
    getter aggregations : Hash(String, Aggregations::Results)?
    # getter aggregations : Hash(String, JSON::Any)?
    getter profile : JSON::Any?
  end

  module Aggregations
    struct Results
      include JSON::Serializable

      getter doc_count_error_upper_bound : Int64?
      getter sum_other_doc_count : Int64?
      getter buckets : Array(Bucket) { [] of Bucket }
    end

    struct Bucket
      include JSON::Serializable

      getter key_as_string : String { "" }
      getter key : JSON::Any::Type
      getter doc_count : Int64
      @[JSON::Field(ignore: true)]
      getter aggregations : Hash(String, Aggregation) { {} of String => Aggregation }

      # getter aggregations : Hash(String, Hash(String, JSON::Any)) { {} of String => Hash(String, JSON::Any) }

      protected def on_unknown_json_attribute(pull, key, key_location)
        aggregations[key] = begin
          Aggregation.from_json(pull)
        rescue exc : ::JSON::ParseException
          raise ::JSON::SerializableError.new(exc.message, self.class.to_s, key, *key_location, exc)
        end
      end

      protected def on_to_json(json)
        aggregations.each do |key, value|
          json.field(key) { value.to_json(json) }
        end
      end
    end

    module Aggregation
      alias Number = Int64 | Float64

      macro included
        include JSON::Serializable
      end

      def self.from_json(pull : JSON::PullParser)
        ({{@type.includers.sort_by(&.name).join(" | ").id}}).new(pull)
      end
    end

    struct ExtendedStats
      include Aggregation

      getter count : Int64
      getter min : Number?
      getter max : Number?
      getter avg : Number?
      getter sum : Number
      getter sum_of_squares : Number?
      getter variance : Number?
      getter variance_population : Number?
      getter variance_sampling : Number?
      getter std_deviation : Number?
      getter std_deviation_population : Number?
      getter std_deviation_sampling : Number?
      getter std_deviation_bounds : Bounds

      struct Bounds
        include JSON::Serializable

        getter upper : Float64?
        getter lower : Float64?
        getter upper_population : Float64?
        getter lower_population : Float64?
        getter upper_sampling : Float64?
        getter lower_sampling : Float64?
      end
    end

    struct Percentiles
      include Aggregation
      getter values : Hash(String, Int64 | Float64 | Nil)

      def [](key : String)
        values[key]
      end

      def []?(key : String)
        values[key]?
      end
    end

    struct Stats
      include Aggregation

      getter count : Int64
      getter min : Int64 | Float64
      getter max : Int64 | Float64
      getter avg : Int64 | Float64
      getter sum : Int64 | Float64
    end

    struct StringStats
      include Aggregation

      getter count : Int64
      getter min_length : Int64?
      getter max_length : Int64?
      getter avg_length : Float64?
      getter entropy : Float64
    end

    struct IntegerValue
      include Aggregation

      getter value : Int64
    end

    struct FloatValue
      include Aggregation

      getter value : Float64
    end

    #
    struct ZNilValue
      include Aggregation

      getter value : Nil
    end

    struct Boxplot
      include Aggregation

      macro field(var)
        @[JSON::Field(converter: ES::Aggregations::Boxplot::ValueConverter.new("{{var.var}}"))]
        getter {{var}}
      end

      field min : Float64
      field max : Float64
      field q1 : Float64
      field q2 : Float64
      field q3 : Float64
      field lower : Float64
      field upper : Float64

      struct ValueConverter
        def initialize(@key : String)
        end

        def from_json(json : JSON::PullParser) : Float64
          if value = json.read?(Float64)
            return value
          end

          case value = json.read?(String)
          when "Infinity"
            Float64::INFINITY
          when "-Infinity"
            -Float64::INFINITY
          when "NaN"
            Float64::NAN
          else
            raise ::JSON::SerializableError.new("Oops: #{value.inspect}", self.class.to_s, @key, *json.location, nil)
          end
        end
      end
    end
  end

  module MillisecondsTimeSpan
    def self.from_json(pull : JSON::PullParser)
      case pull.kind
      when .int?
        pull.read_int.milliseconds
      when .string?
        pull.read_string.to_i64.milliseconds
      else
        raise TypeCastError.new("Cannot convert #{pull.raw_value} to a Time::Span")
      end
    end
  end
end

require "./es"
