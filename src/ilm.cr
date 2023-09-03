require "./client"

module Elasticsearch
  module ILM
    struct Client
      def initialize(@client : Elasticsearch::Client)
      end

      def get_policy(name : String)
        resp = @client.get("_ilm/policy/#{name}") # do |resp|
        # JSON.parse(resp.body_io)
        Hash(String, GetPolicyResponse).from_json resp.body
      end

      def create_policy(name : String, *, phases : Phases, meta = nil)
        request = PutPolicyRequest.new(
          policy: Policy.new(
            meta: meta,
            phases: phases,
          ),
        )

        @client.put("/_ilm/policy/#{name}", body: request.to_json) do |response|
          if response.success?
            response.body_io.skip_to_end
            true
          else
            raise Exception.new "#{response.status}: #{response.body_io.gets_to_end}"
          end
        end
      end

      def delete_policy(name : String) : Nil
        unless (response = @client.delete("/_ilm/policy/#{name}")).success?
          raise Exception.new "#{response.status}: #{response.body}"
        end
      end
    end

    struct PutPolicyRequest
      include JSON::Serializable

      getter policy : Policy

      def initialize(@policy : Policy)
      end
    end

    struct GetPolicyResponse
      include JSON::Serializable

      getter policy : Policy
      getter version : Int64
      getter modified_date : Time
      getter in_use_by : PolicyConsumers

      struct PolicyConsumers
        include JSON::Serializable

        getter indices : Array(String)
        getter data_streams : Array(String)
        getter composable_templates : Array(String)
      end
    end

    struct Policy
      include JSON::Serializable
      alias Metadata = Hash(String, JSON::Any)

      @[JSON::Field(key: "_meta")]
      getter meta : Metadata { Metadata.new }
      getter phases : Phases

      def initialize(*, @phases, @meta = nil)
      end
    end

    struct Phases
      include JSON::Serializable

      getter hot : Phase?
      getter warm : Phase?
      getter cold : Phase?
      getter delete : Phase?

      def initialize(*, @hot = nil, @warm = nil, @cold = nil, @delete = nil)
      end
    end

    struct Phase
      include JSON::Serializable

      @[JSON::Field(converter: Time::Span::Shorthand)]
      getter min_age : Time::Span?
      getter actions : Actions

      def initialize(*, @min_age = nil, @actions)
      end
    end

    struct Actions
      include JSON::Serializable

      getter rollover : RollOver?
      getter downsample : Downsample?
      getter delete : Delete?
      getter shrink : Shrink?
      getter forcemerge : ForceMerge?

      def initialize(
        *,
        @rollover = nil,
        @downsample = nil,
        @delete = nil,
        @shrink = nil,
        @forcemerge = nil
      )
      end

      module Action
        macro included
          include JSON::Serializable
        end
      end

      struct Downsample
        include Action

        @[JSON::Field(converter: Time::Span::Shorthand)]
        getter fixed_interval : Time::Span

        def initialize(*, @fixed_interval)
        end
      end

      struct Shrink
        include Action

        getter number_of_shards : Int64

        def initialize(*, @number_of_shards)
        end
      end

      struct ForceMerge
        include Action

        getter max_num_segments : Int64

        def initialize(*, @max_num_segments)
        end
      end

      struct RollOver
        include Action

        getter min_docs : Int64?
        getter min_size : Size::WithUnit?
        @[JSON::Field(converter: Time::Span::Shorthand)]
        getter min_age : Time::Span?
        getter min_primary_shard_docs : Int64?
        getter min_primary_shard_size : Size::WithUnit?
        getter max_docs : Int64?
        getter max_size : Size::WithUnit?
        @[JSON::Field(converter: Time::Span::Shorthand)]
        getter max_age : Time::Span?
        getter max_primary_shard_docs : Int64?
        getter max_primary_shard_size : Size::WithUnit?

        def initialize(
          *,
          @min_docs = nil,
          @min_size = nil,
          @min_age = nil,
          @min_primary_shard_docs = nil,
          @min_primary_shard_size = nil,
          @max_docs = nil,
          @max_size = nil,
          @max_age = nil,
          @max_primary_shard_docs = nil,
          @max_primary_shard_size = nil
        )
        end
      end

      struct Delete
        include Action

        getter delete_searchable_snapshot : Bool?

        def initialize(*, @delete_searchable_snapshot = nil)
        end
      end
    end
  end

  class Client
    def ilm
      ILM::Client.new(self)
    end
  end
end

require "string_scanner"

struct Time::Span
  module Shorthand
    def self.to_json(span : Time::Span, json : JSON::Builder) : Nil
      json.string span.to_short_s
    end

    def self.from_json(json : JSON::PullParser) : Time::Span
      scanner = StringScanner.new(json.read_string)
      span = 0.seconds
      while (magnitude = scanner.scan(/[0-9]+/).try(&.to_i64?)) && (unit = scanner.scan(/w|d|h|m\b|s|ms|us|ns/))
        span += case unit
                when "w"
                  magnitude.weeks
                when "d"
                  magnitude.days
                when "h"
                  magnitude.hours
                when "m"
                  magnitude.minutes
                when "s"
                  magnitude.seconds
                when "ms"
                  magnitude.milliseconds
                when "us"
                  magnitude.microseconds
                when "ns"
                  magnitude.nanoseconds
                else
                  raise ArgumentError.new("Unknown unit of time: #{unit.inspect}")
                end
      end

      span
    end
  end

  def to_short_s
    String.build { |str| to_short_s str }
  end

  def to_short_s(io, day_marker = 'd', hour_marker = 'h', minute_marker = 'm', second_marker = 's')
    if days > 0
      io << days << day_marker
    end
    if hours > 0
      io << hours << hour_marker
    end
    if minutes > 0
      io << minutes << minute_marker
    end
    if seconds > 0
      io << seconds
      if nanoseconds > 0
        io << '.'
        # Left-pad sub-second values with zeros
        (9 - Math.log(nanoseconds, 10).ceil.to_i).times do
          io << '0'
        end
        io << nanoseconds
      end
      io << second_marker
    end
  end
end
