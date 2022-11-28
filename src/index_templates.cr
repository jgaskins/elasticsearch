require "./client"
require "./types"

module Elasticsearch
  module IndexTemplates
    struct Client
      def initialize(@client : ::ES::Client)
      end

      def create(name : String, index_patterns : Array(String), priority : Int64, template, data_stream = nil)
        response = @client.put "_index_template/#{name}", body: {
          index_patterns: index_patterns,
          data_stream:    data_stream,
          priority:       priority,
          template:       template,
        }.to_json

        if response.success?
          JSON.parse response.body
        else
          raise Exception.new("#{response.status}: #{response.body}")
        end
      end

      def get(name : String)
        response = @client.get("_index_template/#{name}")

        if response.success?
          GetResponse.from_json response.body
        else
          raise Exception.new("#{response.status}: #{response.body}")
        end
      end

      def delete(name : String) : Nil
        response = @client.delete "_index_template/#{name}"
        if response.success?
          true
        else
          raise Exception.new("#{response.status}: #{response.body}")
        end
      end
    end

    struct GetResponse
      include JSON::Serializable

      getter index_templates : Array(Entry)

      struct Entry
        include JSON::Serializable

        getter name : String
        getter index_template : IndexTemplate
      end
    end

    struct IndexTemplate
      include JSON::Serializable

      getter index_patterns : Array(String)
      getter template : Template
      getter composed_of : Array(String)
      getter priority : Int64
      getter data_stream : DataStream

      struct DataStream
        include JSON::Serializable
        getter? hidden : Bool
        getter? allow_custom_routing : Bool
      end

      struct Template
        include JSON::Serializable

        getter mappings : Mapping
      end

      struct Mapping
        include JSON::Serializable

        getter dynamic : String
        getter properties : Hash(String, Property)

        struct Property
          include JSON::Serializable
          # include JSON::Serializable::Unmapped

          getter type : Type
        end
      end
    end
  end

  class Client
    def index_templates
      IndexTemplates::Client.new(self)
    end
  end
end
