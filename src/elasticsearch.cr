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
    end

    include Enumerable(Hit(T))

    def each
      hits.hits.each do |hit|
        yield hit
      end
    end

    @[JSON::Field(converter: ::Elasticsearch::MillisecondsTimeSpan)]
    getter took : Time::Span
    getter timed_out : Bool
    @[JSON::Field(key: "_shards")]
    getter shards : Shards
    getter hits : Hits(T)
    getter aggregations : Hash(String, AggregatedResult)?

    struct AggregatedResult
      include JSON::Serializable

      getter buckets : Array(Bucket)

      struct Bucket
        include JSON::Serializable

        getter key_as_string : String?
        getter key : JSON::Any
        getter doc_count : Int64

        @[JSON::Field(ignore: true)]
        getter aggregations = Hash(String, AggregatedValue).new

        protected def on_unknown_json_attribute(pull, key, key_location)
          aggregations[key] = begin
            AggregatedValue.new(pull)
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

      struct AggregatedValue
        include JSON::Serializable

        getter value : Float64 | Int64 | Nil
        getter! values : Hash(String, Float64 | Int64 | Nil)
      end
    end

    struct Shards
      include JSON::Serializable

      getter total : Int64
      getter successful : Int64
      getter skipped : Int64
      getter failed : Int64
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
