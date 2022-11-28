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
    getter fields : Fields?
    # getter keyword : JSON::Any?
    getter index : Bool?
    getter analyzer : String?

    def initialize(
      @type : Type,
      *,
      @fields = nil,
      @index = nil,
      @analyzer = nil,
    )
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
