require "./client"

module Elasticsearch
  module ILM
    struct Client
      def initialize(@client : Elasticsearch::Client)
      end

      def get_policy(name : String)
        @client.get("_ilm/policy/#{name}") do |resp|
            JSON.parse(resp.body_io)
          # Hash(String, GetPolicyResponse).from_json resp.body_io
        end
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

    struct PutPolicyRequest(T)
      include JSON::Serializable

      getter policy : Policy(T)

      def initialize(@policy : Policy(T))
      end
    end

    struct GetPolicyResponse
      include JSON::Serializable

      getter policy : Policy(Hash(String, JSON::Any))
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

    struct Policy(T)
      include JSON::Serializable

      @[JSON::Field(key: "_meta")]
      getter meta : T
      getter phases : Phases

      def initialize(*, @phases, @meta : T)
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
      getter min_age : Time::Span
      getter actions : Actions::Action

      def initialize(@min_age : Time::Span, @actions : Actions::Action)
      end
    end

    module Actions
      struct Delete
        include JSON::Serializable

        getter delete : Options

        def self.new(**options)
          new(Options.new(**options))
        end

        def initialize(@delete)
        end

        struct Options
          include JSON::Serializable

          getter delete_searchable_snapshot : Bool?

          def initialize(*, @delete_searchable_snapshot = nil)
          end
        end
      end

      alias Action = Delete
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
