require "json"

require "./client"

module Elasticsearch
  enum BulkActionType
    INDEX
    CREATE
    UPDATE
    DELETE
  end

  struct BulkAction(T)
    getter type : BulkActionType
    getter index : String?
    getter id : String?
    getter doc : T?

    def initialize(@doc : T, @type : BulkActionType = :create, @index = nil, @id = nil)
    end

    def to_s(io : IO) : Nil
      JSON.build io do |json|
        json.object do
          json.field type.to_s.downcase do
            json.object do
              json.field "_index", index if index
              json.field "_id", id if id
            end
          end
        end
      end
      io.puts
      if d = doc
        case type
        in .index?, .create?
          d.to_json io
          io.puts
        in .update?
          {doc: d}.to_json io
          io.puts
        in .delete?
          # Do nothing
        end
      end
    end
  end

  struct Documents
    enum Refresh
      TRUE
      FALSE
      WAIT_FOR
    end

    def initialize(@client : Client)
    end

    def index(index_name : String, doc, *, refresh : Refresh = :false)
      params = URI::Params{
        "refresh" => refresh.to_s.downcase,
      }

      response = @client.post "/#{index_name}/_doc?#{params}", doc.to_json
      if response.success?
        # TODO: Make this a concrete type
        JSON.parse(response.body)
      else
        raise Exception.new("#{response.status}: #{response.body}")
      end
    end

    def index(index_name : String, id, doc, *, refresh : Refresh = :false)
      params = URI::Params{
        "refresh" => refresh.to_s.downcase,
      }

      response = @client.post "/#{index_name}/_doc/#{id}?#{params}", doc.to_json
      if response.success?
        # TODO: Make this a concrete type
        JSON.parse(response.body)
      else
        raise Exception.new("#{response.status}: #{response.body}")
      end
    end

    def create(index_name : String, id, doc, *, refresh : Refresh = :false)
      params = URI::Params{
        "refresh" => refresh.to_s.downcase,
      }

      response = @client.post "/#{index_name}/_create/#{id}?#{params}", doc.to_json
      JSON.parse(response.body)
    end

    def bulk(index_name : String, docs : Enumerable, *, refresh : Documents::Refresh = :false)
      bulk index_name, docs, refresh: refresh do |doc|
        BulkAction.new(
          type: :create,
          doc: doc,
          index: index_name,
        )
      end
    end

    def bulk(index_name : String, docs : Enumerable(T), *, refresh : Documents::Refresh = :false, &serialize : T -> BulkAction(T)) forall T
      reader, writer = IO.pipe
      spawn do
        docs.each do |doc|
          serialize.call(doc).to_s(writer)
        end
      ensure
        writer.close
      end
      params = URI::Params{
        "refresh" => refresh.to_s.downcase,
      }

      @client.post "/#{index_name}/_bulk?#{params}", body: reader do |response|
        # JSON.parse response.body
        reader.close
        if response.success?
          # JSON.parse response.body_io
          BulkResponse.from_json(response.body_io)
        else
          raise Exception.new("#{response.status}: #{response.body}")
        end
      end
    end

    def update_by_query(
      index_name : String,
      query,
      *,
      script : Script,
      requests_per_second : Int? = nil,
      conflicts : Conflicts? = nil,
      refresh : Bool = false,
    )
      params = URI::Params{
        "refresh" => refresh.to_s,
      }
      if conflicts
        params["conflicts"] = conflicts.to_s.downcase
      end
      if requests_per_second
        params["requests_per_second"] = requests_per_second.to_s
      end

      request = UpdateByQueryRequest.new(
        query: query,
        script: script,
      )

      @client.post "#{index_name}/_update_by_query?#{params}", request.to_json do |response|
        if response.success?
          UpdateByQueryResponse.from_json(response.body_io)
        else
          raise Exception.new(JSON.parse(response.body_io.gets_to_end).to_s)
        end
      end
    end

    struct UpdateByQueryRequest(T)
      include JSON::Serializable

      getter query : T
      getter script : Script

      def initialize(*, @query, @script)
      end
    end

    struct UpdateByQueryResponse
      include JSON::Serializable

      @[JSON::Field(converter: ::Elasticsearch::MillisecondsTimeSpan)]
      getter took : Time::Span
      getter? timed_out : Bool
      getter total : Int64
      getter updated : Int64
      getter deleted : Int64
      getter batches : Int64
      getter version_conflicts : Int64
      getter noops : Int64
      getter retries : Retries
      @[JSON::Field(key: "throttled_millis", converter: ::Elasticsearch::MillisecondsTimeSpan)]
      getter throttled : Time::Span
      getter requests_per_second : Float64
      @[JSON::Field(key: "throttled_until_millis", converter: ::Elasticsearch::MillisecondsTimeSpan)]
      getter throttled_until : Time::Span
      getter failures : Array(JSON::Any)

      struct Retries
        include JSON::Serializable
        getter bulk : Int64
        getter search : Int64
      end
    end

    enum Conflicts
      Proceed
      Abort
    end

    struct BulkResponse
      include JSON::Serializable

      @[JSON::Field(converter: ::Elasticsearch::MillisecondsTimeSpan)]
      getter took : Time::Span
      getter? errors : Bool
      getter items : Array(Action)

      struct Action
        getter index = ""
        getter id = ""
        getter version = 0i64
        # getter result = ""
        getter status = 0i64
        getter seq_no = 0i64
        getter primary_term = 0i64
        getter shards = Indices::Stats::Shards.new(0i64, 0i64, 0i64)

        def initialize(json : JSON::PullParser)
          json.read_begin_object
          @type = Type.new(json)
          json.read_object do |key|
            case key
            when "_index"   then @index = json.read_string
            when "_id"      then @id = json.read_string
            when "_version" then @version = json.read_int
              # when "result"        then @result = HTTP::Status.new(json)
            when "_shards"       then @shards = Indices::Stats::Shards.new(json)
            when "status"        then @status = HTTP::Status.new(json.read_int.to_i32)
            when "_seq_no"       then @seq_no = json.read_int
            when "_primary_term" then @primary_term = json.read_int
            end
          end
          json.read_end_object
        end

        enum Type
          INDEX
          CREATE
          UPDATE
          DELETE
        end
      end
    end

    def get(
      index_name : String,
      id,
      *,
      source : String = "",
      version : Int? = nil,
      source_includes : String = "",
      source_excludes : String = "",
      as type : T.class
    ) forall T
      params = URI::Params.new
      params["_source"] = source if source.presence
      params["_source_includes"] = source_includes if source_includes.presence
      params["_source_excludes"] = source_excludes if source_excludes.presence
      params["version"] = version.to_s if version

      response = @client.get "/#{index_name}/_doc/#{id}?#{params}"
      if response.success?
        DocumentResult(T).from_json response.body
      elsif response.status.not_found?
        nil
      else
        raise Exception.new("#{response.status}: #{response.body}")
      end
    end

    struct DocumentResult(T)
      include JSON::Serializable

      @[JSON::Field(key: "_index")]
      getter index : String
      # @[JSON::Field(key: "_type")]
      # getter type : String
      @[JSON::Field(key: "_id")]
      getter id : String
      @[JSON::Field(key: "_version")]
      getter version : Int64 = 0
      @[JSON::Field(key: "_seq_no")]
      getter seq_no : Int64 = 0
      getter? found : Bool
      @[JSON::Field(key: "_primary_term")]
      getter primary_term : Int64 = 0
      @[JSON::Field(key: "_source")]
      getter source : T
    end
  end

  class Client
    def docs
      documents
    end

    def documents
      Documents.new(self)
    end
  end
end
