require "json"

module Elasticsearch
  module MillisecondsTime
    def self.from_json(pull : JSON::PullParser)
      case pull.kind
      when .int?
        Time::UNIX_EPOCH + pull.read_int.milliseconds
      when .string?
        Time::UNIX_EPOCH + pull.read_string.to_i64.milliseconds
      else
        raise TypeCastError.new("Cannot convert #{pull.raw_value} to a Time")
      end
    end

    def self.to_json(value : Time, json : JSON::Builder)
      json.number (value - Time::UNIX_EPOCH).total_milliseconds.to_i64
    end
  end

  module StringifiedInt
    def self.from_json(pull : JSON::PullParser)
      pull.read_string.to_i64
    end

    def self.to_json(value : Int, json : JSON::Builder)
      json.string value.to_s
    end
  end
end
