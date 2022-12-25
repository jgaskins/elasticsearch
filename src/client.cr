require "http"
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
    max_idle_connections = 25
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
      http = HTTP::Client.new(@uri.host || "localhost", @uri.port || 9200, tls: tls)
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
    from = nil,
    sort = nil,
    size = nil,
    aggregations = nil,
    as type : T.class = JSON::Any
  ) forall T
    search index_name,
      query: {match_all: match_all},
      from: from,
      sort: sort,
      size: size,
      as: type
  end

  def search(
    index_name : String | Enumerable(String),
    *,
    simple_query_string query : String,
    default_operator = nil,
    analyzer = nil,
    from = nil,
    sort = nil,
    size = nil,
    aggregations = nil,
    as type : T.class = JSON::Any
  ) forall T
    search index_name,
      query: {
        simple_query_string: SimpleQueryStringOptions.new(
          query: query,
          default_operator: default_operator,
          analyzer: analyzer,
        ),
      },
      from: from,
      size: size,
      sort: sort,
      as: type
  end

  record SimpleQueryStringOptions, query : String, default_operator : String? = nil, analyzer : String? = nil do
    include JSON::Serializable
  end

  def search(
    index_name : String | Enumerable(String),
    *,
    query_string query : String,
    query_string_options = NamedTuple.new,
    from = nil,
    sort = nil,
    size = nil,
    aggregations = nil,
    as type : T.class = JSON::Any
  ) forall T
    search index_name,
      query: {
        query_string: QueryString.new(**query_string_options, query: query),
      },
      from: from,
      size: size,
      sort: sort,
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
    size : Int? = nil,
    aggregations = nil,
    sort = nil
  ) forall T
    if index_name.is_a? Enumerable
      index_name = index_name.join(',')
    end
    body = SearchQuery.new(
      from: from,
      size: size,
      query: query,
      aggregations: aggregations,
      sort: sort,
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
    getter size : Int64 | Int32?
    getter query : QueryType
    getter aggregations : AggregationType?
    getter sort : Sort | Array(Sort) | Nil

    def initialize(
      @query : QueryType,
      @from = nil,
      @size = nil,
      @aggregations = nil,
      @sort : Sort | Array(Sort) | Nil = nil
    )
    end
  end

  def get(path : String, &block : HTTP::Client::Response ->)
    @pool.checkout(&.get(path) { |resp| yield resp })
  end

  def put(path : String, body : String? = nil, &block : HTTP::Client::Response ->)
    @pool.checkout(&.put(path, body: body) { |resp| yield resp })
  end

  def get(path : String)
    @pool.checkout(&.get(path))
  end

  def post(path : String, body : String | IO)
    @pool.checkout(&.post(path, body: body))
  end

  def post(path : String, body : String | IO)
    @pool.checkout(&.post(path, body: body) do |response|
      yield response
    ensure
      response.body_io.skip_to_end
    end)
  end

  def put(path : String, body : String? = nil)
    @pool.checkout(&.put(path, body: body))
  end

  def delete(path : String)
    @pool.checkout(&.delete(path))
  end
end

alias ES = Elasticsearch

module Elasticsearch
  alias Sort = Hash(String, String | Hash(String, String))
end
