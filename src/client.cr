require "http/client"
require "openssl"
require "db/pool"
require "json"
require "uri"

require "./errors"
require "./query"

class Elasticsearch::Client
  def initialize(
    @uri : URI = URI.parse(ENV.fetch("ELASTICSEARCH_URL", "http://localhost:9200/")),
    tls : HTTP::Client::TLSContext = uri.scheme == "https",
    max_idle_connections = 25,
    @retries = 5,
    @log = Log.for("elasticsearch")
  )
    options = {
      max_idle_pool_size: max_idle_connections,
    }

    if ca_cert = ENV["ELASTICSEARCH_HTTPS_CA"]?
      case tls
      when true
        tls = OpenSSL::SSL::Context::Client.new
        tls.ca_certificates = ca_cert
      when OpenSSL::SSL::Context::Client
        tls.ca_certificates ||= ca_cert
      end
    end

    @pool = DB::Pool(HTTP::Client).new(**options) do
      http = HTTPClient.new(@uri.host || "localhost", @uri.port || 9200, tls: tls)
      if (user = uri.user) && (password = uri.password)
        http.basic_auth user, password
      end
      http.before_request do |request|
        request.headers.add "content-type", "application/json"
        request.headers.add "connection", "keep-alive"
      end
      http
    end
  end

  def search(
    index_name : String | Enumerable(String),
    *,
    match_all,
    fields = nil,
    from = nil,
    sort = nil,
    size = nil,
    aggregations = nil,
    profile = nil,
    as type : T.class = JSON::Any
  ) forall T
    search index_name,
      query: {match_all: match_all},
      from: from,
      fields: fields,
      sort: sort,
      aggregations: aggregations,
      size: size,
      profile: profile,
      as: type
  end

  def search(
    index_name : String | Enumerable(String),
    *,
    simple_query_string query : String,
    default_operator = nil,
    analyzer = nil,
    fields : Array(String)? = nil,
    aggregations = nil,
    from = nil,
    profile = nil,
    size = nil,
    sort = nil,
    source = nil,
    as type : T.class = JSON::Any
  ) forall T
    search index_name,
      query: {
        simple_query_string: SimpleQueryStringOptions.new(
          query: query,
          default_operator: default_operator,
          analyzer: analyzer,
          fields: fields,
        ),
      },
      from: from,
      profile: profile,
      size: size,
      sort: sort,
      source: source,
      as: type
  end

  record SimpleQueryStringOptions,
    query : String,
    default_operator : String? = nil,
    analyzer : String? = nil,
    fields : Array(String)? = nil do
    include JSON::Serializable
  end

  def search(
    index_name : String | Enumerable(String),
    *,
    query_string query : String,
    query_string_options = NamedTuple.new,
    aggregations = nil,
    fields = nil,
    from = nil,
    profile = nil,
    size = nil,
    sort = nil,
    source = nil,
    as type : T.class = JSON::Any
  ) forall T
    search index_name,
      query: {
        query_string: QueryString.new(**query_string_options, query: query),
      },
      from: from,
      fields: fields,
      size: size,
      sort: sort,
      source: source,
      aggregations: aggregations,
      profile: profile,
      as: type
  end

  record QueryString,
    query : String,
    default_field : String? = nil,
    allow_leading_wildcard : Bool? = nil,
    analyze_wildcard : Bool? = nil,
    analyzer : String? = nil,
    auto_generate_synonyms_phrase_query : Bool? = nil,
    boost : Float64? = nil,
    default_operator : String? = nil,
    enable_position_increments : Bool? = nil,
    fields : Array(String)? = nil,
    fuzziness : String? = nil,
    fuzzy_max_expansions : Int32? = nil,
    fuzzy_prefix_length : Int32? = nil,
    fuzzy_transpositions : Bool? = nil,
    lenient : Bool? = nil,
    max_determinized_states : Int32? = nil,
    minimum_should_match : String? = nil,
    phrase_slop : Int32? = nil,
    quote_analyzer : String? = nil,
    quote_field_suffix : String? = nil,
    rewrite : String? = nil,
    time_zone : String? = nil do
    include JSON::Serializable
  end

  def search(
    index_name : String | Enumerable(String),
    query,
    *,
    as type : T.class,
    from : Int? = nil,
    fields : Array(String)? = nil,
    size : Int? = nil,
    source : String | Bool | Nil = nil,
    aggregations = nil,
    sort = nil,
    track_scores = nil,
    profile = nil
  ) forall T
    if index_name.is_a? Enumerable
      index_name = index_name.join(',')
    end
    body = SearchQuery.new(
      aggregations: aggregations,
      fields: fields,
      from: from,
      profile: profile,
      query: query,
      size: size,
      sort: sort,
      track_scores: track_scores,
      source: source,
    )

    post("#{index_name}/_search", body: body.to_json) do |response|
      if response.success?
        SearchResult(T).from_json response.body_io
      else
        raise Exception.new "#{response.status}: #{JSON.parse response.body_io.gets_to_end}"
      end
    end
  end

  struct SearchQuery(QueryType, AggregationType)
    include JSON::Serializable

    getter from : Int64 | Int32?
    getter fields : Array(String)?
    getter size : Int64 | Int32?
    getter query : QueryType
    getter aggregations : AggregationType?
    @[JSON::Field(key: "_source")]
    getter source : String | Bool | Nil
    getter sort : Sort | Array(Sort) | Nil
    getter? track_scores : Bool?
    getter? profile : Bool?

    def initialize(
      @query : QueryType,
      @from = nil,
      @fields = nil,
      @size = nil,
      @aggregations = nil,
      @source = nil,
      @sort = nil,
      @track_scores = nil,
      @profile = nil
    )
    end
  end

  def refresh
    get("_refresh") do |response|
      if response.success?
        RefreshResponse.from_json(response.body_io).tap do
          response.body_io.skip_to_end
        end
      else
        raise Exception.new "#{response.status}: #{JSON.parse response.body_io.gets_to_end}"
      end
    end
  end

  struct RefreshResponse
    include JSON::Serializable

    @[JSON::Field(key: "_shards")]
    getter shards : Shards
  end

  def reindex(source : String, dest destination : String)
    response = post "_reindex", {
      source: source,
      dest:   destination,
    }.to_json

    JSON.parse(response.body)
  end

  def get(path : String, &block : HTTP::Client::Response ->)
    checkout(&.get(path) { |resp| yield resp })
  end

  def put(path : String, body : String? = nil, &block : HTTP::Client::Response ->)
    checkout(&.put(path, body: body) { |resp| yield resp })
  end

  def get(path : String)
    checkout(&.get(path))
  end

  def post(path : String, body : String | IO)
    checkout(&.post(path, body: body))
  end

  def post(path : String, body : String | IO)
    checkout &.post(path, body: body) do |response|
      yield response
    ensure
      response.body_io.skip_to_end
    end
  end

  def put(path : String, body : String? = nil)
    checkout(&.put(path, body: body))
  end

  def delete(path : String)
    checkout(&.delete(path))
  end

  private def checkout(& : HTTP::Client -> T) forall T
    @pool.checkout do |http|
      result = uninitialized T

      @retries.times do |retry_count|
        result = yield http
        break
      rescue ex : IO::Error
        @log.error { ex }
        raise ex if retry_count == @retries - 1
      end

      result
    end
  end
end

module Elasticsearch
  alias Sort = Hash(String, String | Hash(String, String) | SortScript)

  struct SortScript
    include JSON::Serializable

    getter type : String
    getter script : Script
    getter order : SortDirection

    def initialize(*, @type, @script, @order)
    end

    enum SortDirection
      ASC
      DESC
    end
  end

  struct Script
    include JSON::Serializable

    alias Params = Hash(String, JSON::Any::Type)

    getter lang : String
    getter source : String
    getter params : Params

    def initialize(*, @source, @params = nil, @lang = "painless")
    end
  end

  struct Shards
    include JSON::Serializable

    getter total : Int64
    getter successful : Int64 = 0
    getter skipped : Int64 = 0
    getter failed : Int64 = 0
  end

  struct ReindexSource
    include JSON::Serializable

    getter index : String

    def initialize(@index)
    end
  end

  struct ReindexDestination
    include JSON::Serializable

    getter index : String
    getter op_type : OpType?

    def initialize(@index, @op_type = nil)
    end

    enum OpType
      INDEX
      CREATE
    end
  end

  class HTTPClient < ::HTTP::Client
    Log = ::Log.for(Elasticsearch)

    protected def around_exec(request, &)
      start = Time.monotonic
      begin
        yield
      ensure
        duration = Time.monotonic - start
        Log.debug &.emit "query",
          method: request.method,
          host: host,
          resource: request.resource,
          body: request.body.to_s,
          duration_sec: duration.total_seconds
      end
    end
  end
end
