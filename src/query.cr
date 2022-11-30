module Elasticsearch
  def self.query(*, bool)
    Query::Query.new(
      bool: bool,
    )
  end

  def self.bool(*, must = nil, filter = nil, should = nil, must_not = nil)
    Query::Bool.new(
      must: must,
      filter: filter,
      should: should,
      must_not: must_not,
    )
  end

  def self.range(field : String, **kwargs)
    Query::Range.new(field, **kwargs)
  end

  def self.filter(*filters)
    Query::Filter.new(filters.map(&.as(Query::Filterable)).to_a)
  end

  def self.must(*filters)
    Query::Filter.new(filters.map(&.as(Query::Filterable)).to_a)
  end

  def self.match_phrase(**kwargs)
    name = kwargs.keys.first
    value = kwargs.values.first
    Query::MatchPhrase.new(name.to_s, value)
  end

  def self.term(field, value)
    Query::Term.new(field, value)
  end

  def self.aggregations(**kwargs : Query::Aggregations::Aggregation)
    hash = Hash(String, Query::Aggregations::Aggregation).new(initial_capacity: kwargs.size)
    kwargs.each do |key, value|
      hash[key.to_s] = value
    end
    Query::Aggregations.new(hash)
  end

  def self.aggregation(min : Query::Aggregations::Min)
    Query::Aggregations::BasicAggregation.new(min: min)
  end

  def self.aggregation(avg : Query::Aggregations::Avg)
    Query::Aggregations::BasicAggregation.new(avg: avg)
  end

  def self.aggregation(max : Query::Aggregations::Max)
    Query::Aggregations::BasicAggregation.new(max: max)
  end

  def self.aggregation(percentiles : Query::Aggregations::Percentiles)
    Query::Aggregations::BasicAggregation.new(percentiles: percentiles)
  end

  def self.aggregation(terms : NamedTuple, size : Int32? = nil)
    Query::Aggregations::TermsAggregation.new(terms: terms, size: size)
  end

  def self.aggregation(date_histogram : Query::Aggregations::DateHistogram, aggregations : Query::Aggregations)
    Query::Aggregations::BasicAggregation.new(
      date_histogram: date_histogram,
      aggregations: aggregations,
    )
  end

  def self.date_histogram(*, field, fixed_interval = nil, calendar_interval = nil, missing = nil)
    Query::Aggregations::DateHistogram.new(
      field: field,
      fixed_interval: fixed_interval,
      calendar_interval: calendar_interval,
      missing: missing,
    )
  end

  def self.min(field : String)
    Query::Aggregations::Min.new(field)
  end

  def self.avg(field : String)
    Query::Aggregations::Avg.new(field)
  end

  def self.max(field : String)
    Query::Aggregations::Max.new(field)
  end

  def self.percentiles(field : String, percents : Array(String | Int | Float))
    percents = percents.map do |p|
      case p
      in String, Float
        p.to_f64
      in Int
        p.to_i64
      end
    end
    Query::Aggregations::Percentiles.new(field: field, percents: percents)
  end

  module Query
    module Filterable
      macro included
        include JSON::Serializable
      end
    end

    struct Query
      include Filterable

      getter bool : Bool?

      def initialize(*, @bool)
      end
    end

    struct Bool
      include Filterable

      getter must : Filter?
      getter filter : Filter?
      getter should : Filter?
      getter must_not : Filter?

      def initialize(*, @must = nil, @filter = nil, @should = nil, @must_not = nil)
      end
    end

    struct Filter
      include JSON::Serializable

      getter terms : Array(Filterable)

      def initialize(@terms)
      end

      def <<(term : Filterable)
        @terms << term
      end

      def to_json(json : JSON::Builder)
        @terms.to_json json
      end
    end

    struct Range
      include Filterable

      getter field : String
      getter gte : JSON::Any::Type?
      getter gt : JSON::Any::Type?
      getter lt : JSON::Any::Type?
      getter lte : JSON::Any::Type?
      getter format : String?
      getter relation : String?
      getter time_zone : String?
      getter boost : Float64?

      def initialize(
        @field,
        @gte = nil,
        @gt = nil,
        @lt = nil,
        @lte = nil,
        @boost = nil,
        @format = nil,
        @relation = nil,
        @time_zone = nil
      )
      end

      def to_json(json : JSON::Builder)
        json.object do
          json.field "range" do
            json.object do
              json.field @field do
                json.object do
                  {% for property in %w[gte gt lt lte format relation time_zone boost] %}
                    json.field {{property}}, {{property.id}} if {{property.id}}
                  {% end %}
                end
              end
            end
          end
        end
      end
    end

    struct Term
      include Filterable

      getter name : String
      getter value : JSON::Any::Type?

      def initialize(@name, @value)
      end

      def to_json(json : JSON::Builder)
        json.object do
          json.field "term" do
            json.object do
              json.field @name do
                json.object do
                  json.field "value", value if value
                end
              end
            end
          end
        end
      end
    end

    struct MatchPhrase
      include Filterable

      getter name : String
      getter value : JSON::Any::Type?

      def initialize(@name, @value)
      end

      def to_json(json : JSON::Builder)
        json.object do
          json.field "match_phrase" do
            json.object do
              json.field @name, @value
            end
          end
        end
      end
    end

    struct Aggregations
      include JSON::Serializable

      getter aggregations : Hash(String, Aggregation)

      def initialize(@aggregations)
      end

      def to_json(json : JSON::Builder)
        aggregations.to_json json
      end

      module Aggregation
        macro included
          include JSON::Serializable
        end
      end

      struct BasicAggregation
        include Aggregation

        getter min : Min?
        getter avg : Avg?
        getter max : Max?
        getter percentiles : Percentiles?
        getter date_histogram : DateHistogram?
        getter aggregations : Aggregations?

        def initialize(
          @min = nil,
          @avg = nil,
          @max = nil,
          @percentiles = nil,
          @date_histogram = nil,
          @aggregations = nil
        )
        end
      end

      struct TermsAggregation(Terms)
        include Aggregation

        getter terms : Terms
        getter size : Int32?

        def initialize(@terms, @size)
        end
      end

      struct DateHistogram
        include JSON::Serializable

        def initialize(
          @field : String,
          @fixed_interval : String? = nil,
          @calendar_interval : String? = nil,
          @missing : String? = nil
        )
        end
      end

      record(Min, field : String) { include JSON::Serializable }
      record(Max, field : String) { include JSON::Serializable }
      record(Avg, field : String) { include JSON::Serializable }
      record(Percentiles, field : String, percents : Array(Int64 | Float64)) { include JSON::Serializable }
    end
  end
end
