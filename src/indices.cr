require "json"

require "./client"
require "./mappings"

module Elasticsearch
  module Indices
    alias GetResponse = Hash(String, Index)

    struct Client
      def initialize(@client : ::Elasticsearch::Client)
      end

      def create(name : String)
        @client.put sanitize(name)
      end

      def create(name : String, mappings : Mapping, settings = nil)
        @client.put sanitize(name), body: CreateRequest.new(
          mappings: mappings,
          settings: settings,
        ).to_json
      end

      private struct CreateRequest(T)
        include JSON::Serializable

        getter mappings : Mapping
        getter settings : T

        def initialize(@mappings, @settings : T)
        end
      end

      def get(names : Enumerable(String))
        get names.join(',')
      end

      def get(name : String)
        @client.get sanitize(name) do |response|
          if response.success?
            GetResponse.from_json response.body_io
          else
            raise Exception.new("#{response.status}: #{JSON.parse response.body_io.gets_to_end}")
          end
        end
      end

      def delete(name : String)
        @client.delete sanitize(name)
      end

      def stats(name : String)
        @client.get("#{sanitize name}/_stats") do |response|
          Stats.from_json response.body_io
        end
      end

      # Sanitize index names to account for things like date math. See:
      # https://www.elastic.co/guide/en/elasticsearch/reference/current/api-conventions.html#api-date-math-index-names
      private def sanitize(name : String)
        # If there are no reserved bytes, we don't allocate another string
        if name.to_slice.all? { |byte| URI.unreserved? byte }
          name
        else
          URI.encode_path name
        end
      end
    end

    struct Index
      include JSON::Serializable
      # TODO: Figure out what aliases look like
      # getter aliases : Hash(String, String)?
      getter mappings : Mapping
      getter settings : Settings
    end

    struct Settings
      include JSON::Serializable

      getter index : Index?

      def initialize(@index = nil)
      end

      struct Index
        include JSON::Serializable

        getter analysis : Analysis?
        getter routing : Routing { Routing.new }
        getter hidden : Hidden { Hidden::False }
        @[JSON::Field(converter: ::ES::StringifiedInt)]
        getter number_of_shards : Int64 { 1i64 }
        @[JSON::Field(converter: ::ES::StringifiedInt)]
        getter number_of_replicas : Int64 { 1i64 }
        getter provided_name : String { "" }
        @[JSON::Field(key: "creation_date", converter: ::Elasticsearch::MillisecondsTime)]
        getter creation_date : Time { Time.utc(seconds: 0, nanoseconds: 0) }
        getter uuid : String { "" }
        getter version : Version { Version.new }
        getter lifecycle : Lifecycle?

        def initialize(
          @analysis = nil,
          @routing = nil,
          @hidden = nil,
          @number_of_shards = nil,
          @number_of_replicas = nil,
          @provided_name = nil,
          @creation_date = nil,
          @uuid = nil,
          @version = nil,
          @lifecycle = nil
        )
        end

        enum Hidden
          True
          False
        end

        struct Lifecycle
          include JSON::Serializable

          getter name : String?
        end

        struct Routing
          include JSON::Serializable

          getter allocation : Allocation

          struct Allocation
            include JSON::Serializable

            getter include : Include

            struct Include
              include JSON::Serializable

              @[JSON::Field(key: "_tier_preference")]
              getter tier_preference : TierPreference

              enum TierPreference
                # TODO: What are all the values here?
                DataHot
                DataWarm
                DataCold
                DataFreeze
                DataContent
              end
            end
          end
        end

        struct Version
          include JSON::Serializable

          getter created : String
        end
      end

      struct Analysis
        include JSON::Serializable

        getter analyzer : String | AnalyzerMap | Nil
        getter filter : FilterMap?

        def initialize(@analyzer = nil, @filter = nil)
        end

        alias AnalyzerMap = Hash(String, Analyzer)
        alias FilterMap = Hash(String, Filter)

        abstract struct Filter
          include JSON::Serializable

          getter type : Type

          def initialize(@type : Type)
          end

          def self.new(*, stopwords : String)
            Stop.new(stopwords)
          end

          def self.new(*, keywords : Array(String))
            KeywordMarker.new(keywords)
          end

          def self.new(*, language : String)
            Stemmer.new(language)
          end

          use_json_discriminator "type", {
            stop:           Stop,
            keyword_marker: KeywordMarker,
            stemmer:        Stemmer,
          }

          enum Type
            STOP
            KEYWORD_MARKER
            STEMMER
          end
        end

        struct Stop < Filter
          getter stopwords : String

          def self.new(stopwords : String)
            new(:stop, stopwords)
          end

          def initialize(@type : Type, @stopwords)
          end
        end

        struct KeywordMarker < Filter
          getter keywords : Array(String)

          def self.new(keywords : Array(String))
            new :keyword_marker, keywords
          end

          def initialize(@type : Type, @keywords)
          end
        end

        struct Stemmer < Filter
          getter language : String

          def self.new(language : String)
            new :stemmer, language
          end

          def initialize(@type : Type, @language)
          end
        end
      end

      struct Analyzer
        include JSON::Serializable

        getter tokenizer : String?
        getter filter : Array(String)?

        def initialize(@tokenizer = nil, @filter = nil)
        end
      end
    end

    struct Stats
      include JSON::Serializable

      @[JSON::Field(key: "_shards")]
      getter shards : Shards
      @[JSON::Field(key: "_all")]
      getter all : AllIndices
      getter indices : Hash(String, Index)

      module Stats
        getter primaries : Primaries
      end

      struct AllIndices
        include JSON::Serializable
        include Stats
      end

      struct Index
        include JSON::Serializable
        include Stats
        getter uuid : String
      end

      struct Shards
        include JSON::Serializable

        getter total : Int64
        getter successful : Int64
        getter failed : Int64

        def initialize(@total, @successful, @failed)
        end
      end

      struct Primaries
        include JSON::Serializable

        # If the index doesn't exist, the `all` property of the Stats object
        # will be an empty, so we need to provide defaults all the way down the
        # tree from here to account for querying stats for indexes that no
        # longer exist.

        getter docs : Docs = Docs.new
        getter shard_stats : ShardStats = ShardStats.new
        getter store : Store = Store.new
        getter indexing : Indexing = Indexing.new
        getter get : Get = Get.new
        getter search : Search = Search.new
        getter merges : Merges = Merges.new
        getter refresh : Refresh = Refresh.new
      end

      struct Docs
        include JSON::Serializable

        getter count : Int64 = 0i64
        getter deleted : Int64 = 0i64

        def initialize
        end
      end

      struct ShardStats
        include JSON::Serializable

        getter total_count : Int64 = 0i64

        def initialize
        end
      end

      struct Store
        include JSON::Serializable

        getter size_in_bytes : Int64 = 0i64
        getter total_data_set_size_in_bytes : Int64 = 0i64
        getter reserved_in_bytes : Int64 = 0i64

        def initialize
        end
      end

      private module Macros
        macro stat_getter(name, total = true, time = true, current = false, failed = false)
          {% if total %}
            getter {{name.id}}_total : Int64 = 0i64
          {% end %}

          {% if time %}
            @[JSON::Field(key: "{{name.id}}_time_in_millis", converter: ::Elasticsearch::MillisecondsTimeSpan)]
            getter {{name.id}}_time : Time::Span = 0.seconds
            {% end %}

          {% if current %}
            getter {{name.id}}_current : Int64 = 0i64
          {% end %}

          {% if failed %}
            getter {{name.id}}_failed : Int64 = 0i64
          {% end %}
        end
      end

      struct Indexing
        include JSON::Serializable

        Macros.stat_getter "index", total: true, time: true, current: true, failed: true
        Macros.stat_getter "delete", total: true, time: true, current: true, failed: false
        getter noop_update_total : Int64 = 0i64
        @[JSON::Field(key: "is_throttled")]
        getter? throttled : Bool = false
        Macros.stat_getter "throttle", total: false, time: true, current: false, failed: false

        def initialize
        end
      end

      struct Get
        include JSON::Serializable

        getter total : Int64 = 0i64
        @[JSON::Field(key: "time_in_millis", converter: ::Elasticsearch::MillisecondsTimeSpan)]
        getter time : Time::Span = 0.seconds
        Macros.stat_getter "exists", total: true, time: true
        Macros.stat_getter "missing", total: true, time: true
        getter current : Int64 = 0i64

        def initialize
        end
      end

      struct Search
        include JSON::Serializable

        getter open_contexts : Int64 = 0i64
        Macros.stat_getter "query", total: true, time: true, current: true
        Macros.stat_getter "fetch", total: true, time: true, current: true
        Macros.stat_getter "scroll", total: true, time: true, current: true
        Macros.stat_getter "suggest", total: true, time: true, current: true

        def initialize
        end
      end

      struct Merges
        include JSON::Serializable

        getter current : Int64 = 0i64
        getter current_docs : Int64 = 0i64
        getter current_size_in_bytes : Int64 = 0i64
        getter total : Int64 = 0i64
        Macros.stat_getter "total", total: false, time: true
        getter total_docs : Int64 = 0i64
        getter total_size_in_bytes : Int64 = 0i64
        Macros.stat_getter "total_stopped", total: false, time: true
        Macros.stat_getter "total_throttled", total: false, time: true
        getter total_auto_throttle_in_bytes : Int64 = 0i64

        def initialize
        end
      end

      struct Refresh
        include JSON::Serializable

        getter total : Int64 = 0i64
        Macros.stat_getter "total", total: false, time: true
        Macros.stat_getter "external", time: false
        Macros.stat_getter "external_total", total: false
        getter listeners : Int64 = 0i64

        def initialize
        end
      end
    end
  end

  class Client
    @[Deprecated("Elasticsearch uses `indices` as the plural for 'index', so please Use the `indices` method instead.")]
    def indexes
      indices
    end

    def indices
      Indices::Client.new(self)
    end
  end
end
