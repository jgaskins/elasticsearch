require "json"
require "./ext/json/to_json"

require "./client"
require "./documents"

module Elasticsearch
  struct DataStreams
    def initialize(@client : Client)
    end

    def create!(name : String)
      create name
      get(name).first
    end

    def create(name : String)
      response = @client.put "/_data_stream/#{name}"
      if response.success?
        DataStreamCreated.new(name, @client)
      else
        raise Exception.new("#{response.status}: #{response.body}")
      end
    end

    def delete(stream : DataStream)
      delete stream.name
    end

    def delete(name : String)
      response = @client.delete "/_data_stream/#{name}"
      if response.success?
        true
      else
        raise Exception.new("#{response.status}: #{JSON.parse response.body}")
      end
    end

    def get(name : String)
      @client.get "_data_stream/#{name}" do |response|
        if response.success?
          DataStreamsResponse.from_json response.body_io
        else
          raise Exception.new("#{response.status}: #{response.body}")
        end
      end
    end

    def stats(name : String)
      @client.get "_data_stream/#{name}/_stats" do |response|
        if response.success?
          StatsResponse.from_json response.body_io
        else
          raise Exception.new("#{response.status}: #{response.body}")
        end
      end
    end

    struct DataStreamCreated
      getter name

      def initialize(@name : String, @client : Client)
      end

      def <<(datapoint : Datapoint)
        @client.documents.index @name, datapoint
      end
    end

    struct DataStream
      include JSON::Serializable

      # https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-data-stream.html#get-data-stream-api-response-body
      getter name : String
      getter timestamp_field : Field
      getter indices : Array(Index)
      getter generation : Int64
      @[JSON::Field(key: "_meta")]
      getter meta : Hash(String, JSON::Any)?
      getter status : Status
      getter template : String
      getter ilm_policy : String?
      getter? hidden : Bool
      getter? system : Bool
      getter? allow_custom_routing : Bool
      getter? replicated : Bool
    end

    struct DataStreamsResponse
      include JSON::Serializable
      include Enumerable(DataStream)

      getter data_streams : Array(DataStream)
      delegate each, to: data_streams
    end

    struct Field
      include JSON::Serializable

      getter name : String
    end

    struct Index
      include JSON::Serializable

      @[JSON::Field(key: "index_name")]
      getter name : String
      @[JSON::Field(key: "index_uuid")]
      getter uuid : String
    end

    enum Status
      Green
      Yellow
      Red
    end

    struct StatsResponse
      include JSON::Serializable

      @[JSON::Field(key: "_shards")]
      getter shards : Shards
      getter data_stream_count : Int64
      getter backing_indices : Int64
      getter total_store_size_bytes : Int64
      getter data_streams : Array(DataStream)

      struct Shards
        include JSON::Serializable

        getter total : Int64
        getter successful : Int64
        getter failed : Int64
      end

      struct DataStream
        include JSON::Serializable

        getter data_stream : String
        getter backing_indices : Int64
        getter store_size_bytes : Int64
        @[JSON::Field(converter: ::Elasticsearch::DataStreams::StatsResponse::DataStream::NanosecondsTimestamp)]
        getter maximum_timestamp : Time

        module NanosecondsTimestamp
          def self.from_json(json : JSON::PullParser) : Time
            Time::UNIX_EPOCH + json.read_int.nanoseconds
          end
        end
      end
    end
  end

  struct Datapoint(T)
    getter timestamp : Time
    getter data : T

    def initialize(@data : T, @timestamp : Time = Time.utc)
    end

    def to_json(json : JSON::Builder)
      json.object do
        json.field "@timestamp", @timestamp.to_rfc3339(fraction_digits: 9)
        @data.to_json_properties json
      end
    end
  end

  class Client
    def data_streams
      DataStreams.new(self)
    end
  end
end
