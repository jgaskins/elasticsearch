require "json"

require "./client"
require "./json_conversion"
require "./types"

module Elasticsearch
  struct Mappings
    def initialize(@client : Client)
    end

    def get(index_name : String)
      @client.get "#{index_name}/_mapping" do |response|
        if response.success?
          Hash(String, MappingResponse).from_json response.body_io
        else
          raise Exception.new("#{response.status}: #{response.body}")
        end
      end
    end

    struct MappingResponse
      include JSON::Serializable

      getter mappings : Mapping
    end
  end

  struct Mapping
    include JSON::Serializable

    getter dynamic : Dynamic?
    @[JSON::Field(key: "_data_stream_timestamp")]
    getter data_stream_timestamp : DataStreamTimestamp?
    getter properties : Properties = Properties.new
    getter runtime : RuntimePropertyMap = RuntimePropertyMap.new

    def initialize(@properties, @dynamic = nil, @runtime = RuntimePropertyMap.new, @data_stream_timestamp = nil)
    end
  end

  enum Dynamic
    True
    False
    Runtime
  end

  alias RuntimePropertyMap = Hash(String, DynamicProperty)

  struct DynamicProperty
    include JSON::Serializable

    getter type : Type
  end

  struct DataStreamTimestamp
    include JSON::Serializable

    getter? enabled : Bool
  end

  alias Properties = Hash(String, Property | Namespace)

  struct Namespace
    include JSON::Serializable

    getter properties : Properties
  end

  struct Property
    include JSON::Serializable

    getter type : Type
    getter format : Format?
    getter fields : Fields?
    # getter keyword : JSON::Any?
    getter index : Bool?
    getter analyzer : String?
    getter time_series_dimension : Bool?
    getter time_series_metric : TimeSeriesMetricType?
    getter properties : Hash(String, NestedProperty)?

    def initialize(
      @type : Type,
      *,
      @format = nil,
      @fields = nil,
      @index = nil,
      @analyzer = nil,
      @time_series_dimension = nil,
      @time_series_metric = nil,
      @properties = nil
    )
    end

    @[Flags]
    enum Format
      StrictDateOptionalTimeNanos
      StrictDateOptionalTime
      EpochMillis

      def self.new(json : JSON::PullParser)
        value = None
        string = json.read_string
        string.split("||").each do |component|
          value |= parse?(component) || json.raise "Unknown enum #{self} value: #{component.inspect}"
        end
        value
      end

      def to_json(json : JSON::Builder)
        i = 0
        string = String.build do |str|
          each do |member, _value|
            if i > 0
              str << "||"
            end

            member.to_s.underscore str
            i += 1
          end
        end

        json.string string
      end
    end

    struct NestedProperty
      include JSON::Serializable

      # def initialize(@
    end

    enum TimeSeriesMetricType
      Counter
      Gauge
    end
  end

  struct Fields
    include JSON::Serializable
    # include JSON::Serializable::Unmapped

    getter keyword : Keyword?
    getter text : Text?

    struct Keyword
      include JSON::Serializable

      getter type : Type
      getter ignore_above : Int64
    end

    struct Text
      include JSON::Serializable

      getter type : Type
    end
  end

  class Client
    def mappings
      Mappings.new(self)
    end
  end
end
