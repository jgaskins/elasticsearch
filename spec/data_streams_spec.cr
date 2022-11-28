require "./spec_helper"
require "uuid"

require "../src/data_streams"
require "../src/index_templates"

es = Elasticsearch::Client.new

module Elasticsearch
  describe DataStreams do
    before_all do
      # Data streams need matching index templates with a `data_stream` property
      es.index_templates.create "test-run",
        index_patterns: %w[test-*],
        data_stream: {hidden: false},
        priority: 1,
        template: {
          mappings: {
            dynamic: true,
          },
        }
    end

    after_all do # you're my wonderwall
      es.index_templates.delete "test-run"
    end

    it "creates and inserts data into a stream" do
      name = "test-#{UUID.random.to_s}"

      stream = es.data_streams.create(name)
      begin
        stream << Datapoint.new(
          timestamp: Time.utc,
          data: {
            user: {
              name: "jamie",
              id:   1234,
            },
          },
        )
      ensure
        es.data_streams.delete stream.name if stream
      end
    end
  end
end
