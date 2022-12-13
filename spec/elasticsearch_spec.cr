require "./spec_helper"
require "uuid/json"
require "faker"

require "../src/elasticsearch"

es = Elasticsearch::Client.new

struct Person
  include JSON::Serializable

  getter id : UUID
  getter name : String
  @[JSON::Field(converter: Time::Format.new("%FT%T.%9N%z"))]
  getter created_at : Time

  def initialize(*, @name, @id = UUID.random, @created_at = Time.utc)
  end
end

require "benchmark"

describe Elasticsearch do
  index = UUID.random.to_s

  before_all do
    # We create the index once because this is *slow*
    es.indices.create index
  end

  after_all do
    es.indices.delete index
  end

  describe ES::Indices do
    it "creates indices" do
      # If the index was not created above, this will raise an exception
      es.indices.get index
    end
  end

  describe ES::Documents do
    it "indexes documents" do
      id = UUID.random.to_s
      person = Person.new(name: Faker::Name.name)

      es.indices.create index
      es.documents.index index, id, person

      result = es.documents.get(index, id, as: Person)

      result.index.should eq index
      result.id.should eq id
      result.source.should be_a Person
      result.source.id.should eq person.id
      result.source.name.should eq person.name
      result.source.created_at.should eq person.created_at
    end
  end

  describe "aggregations" do
    it "parses buckets" do
      json = {
        "key_as_string": "2022-12-04T15:30:00.000Z",
        "key":           1670167800000,
        "doc_count":     1440,
        "percentiles":   {
          "values": {"50.0": 5338902.958874458, "90.0": 7213934.888888889},
        },
      }.to_json

      bucket = ES::SearchResult::Bucket.from_json(json)

      bucket.key_as_string.should eq "2022-12-04T15:30:00.000Z"
      bucket.key.should eq 1670167800000
      bucket.doc_count.should eq 1440
      bucket.aggregations["percentiles"].should be_a ES::SearchResult::MultipleValues
    end

    it "parses nested buckets" do
      json = {
        "key":       "GET /health(.:format)",
        "doc_count": 8638,
        "by_field":  {
          "buckets": [
            {
              "key_as_string": "2022-12-04T15:30:00.000Z",
              "key":           1670167800000,
              "doc_count":     1440,
              "percentiles":   {
                "values": {"50.0": 5338902.958874458, "90.0": 7213934.888888889},
              },
            },
          ],
        },
      }.to_json

      bucket = ES::SearchResult::Bucket.from_json(json)

      bucket.aggregations["by_field"].should be_a ES::SearchResult::NestedBucket
    end

    it "parses search results with aggregations" do
      json = {
        "doc_count_error_upper_bound": 1,
        "sum_other_doc_count":         48,
        "buckets":                     [
          {
            "key":       "GET /health(.:format)",
            "doc_count": 8638,
            "by_field":  {
              "buckets": [
                {
                  "key_as_string": "2022-12-04T15:30:00.000Z",
                  "key":           1670167800000,
                  "doc_count":     1440,
                  "percentiles":   {
                    "values": {"50.0": 5338902.958874458, "90.0": 7213934.888888889},
                  },
                },
              ],
            },
          },
        ],
      }.to_json

      result = ES::SearchResult::TopLevelAggregatedResult.from_json(json)
      bucket = result.buckets.first

      bucket.doc_count.should eq 8638
      bucket
        .aggregations["by_field"].as(ES::SearchResult::NestedBucket)
        .buckets
        .first
        .aggregations["percentiles"].as(ES::SearchResult::MultipleValues)
        .values["50.0"]
        .should eq 5338902.958874458
    end

    it "parses a complex aggregation" do
      null = nil
      json = {
        "doc_count_error_upper_bound": 1,
        "sum_other_doc_count":         48,
        "buckets":                     [
          {
            "key":       "GET /health(.:format)",
            "doc_count": 8638,
            "by_field":  {
              "buckets": [
                {
                  "key_as_string": "2022-12-04T15:30:00.000Z",
                  "key":           1670167800000,
                  "doc_count":     1440,
                  "percentiles":   {
                    "values": {"50.0": 5338902.958874458, "90.0": 7213934.888888889},
                  },
                }, {
                  "key_as_string": "2022-12-04T16: 00: 00.000Z",
                  "key":           1670169600000,
                  "doc_count":     2160,
                  "percentiles":   {
                    "values": {
                      "50.0": 5246730.172649574,
                      "90.0": 7159366.5,
                    },
                  },
                }, {
                  "key_as_string": "2022-12-04T16: 30: 00.000Z", "key": 1670171400000, "doc_count": 2160, "percentiles": {
                    "values": {
                      "50.0": 5370117.060606061, "90.0": 7389613.3,
                    },
                  },
                }, {
                  "key_as_string": "2022-12-04T17: 00: 00.000Z", "key": 1670173200000, "doc_count": 2160, "percentiles": {
                    "values": {
                      "50.0": 5295723.778846154, "90.0": 7111066.785714285,
                    },
                  },
                }, {
                  "key_as_string": "2022-12-04T17: 30: 00.000Z", "key": 1670175000000, "doc_count": 718, "percentiles": {
                    "values": {

                      "50.0": 5400039.5, "90.0": 7152935.5,
                    },
                  },
                },
              ],
            },
          }, {
            "key": "POST /inbox(.: format)", "doc_count": 396, "by_field": {
              "buckets": [{
                "key_as_string": "2022-12-04T15: 30: 00.000Z", "key": 1670167800000, "doc_count": 71, "percentiles": {
                  "values": {
                    "50.0": 1.751876E7, "90.0": 2.980032E7,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 00: 00.000Z", "key": 1670169600000, "doc_count": 102, "percentiles": {
                  "values": {
                    "50.0": 1.62533085E7, "90.0": 2.5826337700000003E7,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 30: 00.000Z", "key": 1670171400000, "doc_count": 86, "percentiles": {
                  "values": {
                    "50.0": 1.6354669E7, "90.0": 2.53735205E7,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T17: 00: 00.000Z", "key": 1670173200000, "doc_count": 102, "percentiles": {
                  "values": {
                    "50.0": 1.65499685E7, "90.0": 2.57744515E7,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T17: 30: 00.000Z", "key": 1670175000000, "doc_count": 35, "percentiles": {
                  "values": {
                    "50.0": 1.6367307E7, "90.0": 2.6795081E7,
                  },
                },
              }],
            },
          }, {
            "key": "HTTP GET", "doc_count": 22, "by_field": {
              "buckets": [{
                "key_as_string": "2022-12-04T15: 30: 00.000Z", "key": 1670167800000, "doc_count": 13, "percentiles": {
                  "values": {
                    "50.0": 511981.0, "90.0": 1306092.0000000014,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 00: 00.000Z", "key": 1670169600000, "doc_count": 0, "percentiles": {
                  "values": {
                    "50.0": null, "90.0": null,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 30: 00.000Z", "key": 1670171400000, "doc_count": 8, "percentiles": {
                  "values": {
                    "50.0": 531472.5, "90.0": 1166938.7000000002,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T17: 00: 00.000Z", "key": 1670173200000, "doc_count": 0, "percentiles": {
                  "values": {
                    "50.0": null, "90.0": null,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T17: 30: 00.000Z", "key": 1670175000000, "doc_count": 1, "percentiles": {
                  "values": {
                    "50.0": 378732.0, "90.0": 378732.0,
                  },
                },
              }],
            },
          }, {
            "key": "GET /nodeinfo/2.0(.: format)", "doc_count": 17, "by_field": {
              "buckets": [{
                "key_as_string": "2022-12-04T15: 30: 00.000Z", "key": 1670167800000, "doc_count": 13, "percentiles": {
                  "values": {
                    "50.0": 4746267.0, "90.0": 9425596.600000016,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 00: 00.000Z", "key": 1670169600000, "doc_count": 1, "percentiles": {
                  "values": {
                    "50.0": 6745340.0, "90.0": 6745340.0,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 30: 00.000Z", "key": 1670171400000, "doc_count": 0, "percentiles": {
                  "values": {
                    "50.0": null, "90.0": null,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T17: 00: 00.000Z", "key": 1670173200000, "doc_count": 3, "percentiles": {
                  "values": {
                    "50.0": 6489865.0, "90.0": 1.4050082E7,
                  },
                },
              }],
            },
          }, {
            "key": "GET /api/v1/directory", "doc_count": 16, "by_field": {
              "buckets": [{
                "key_as_string": "2022-12-04T15: 30: 00.000Z", "key": 1670167800000, "doc_count": 5, "percentiles": {
                  "values": {
                    "50.0": 7.3630838E7, "90.0": 8.4856272E8,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 00: 00.000Z", "key": 1670169600000, "doc_count": 9, "percentiles": {
                  "values": {
                    "50.0": 8.3454748E7, "90.0": 6.378252622E8,
                  },
                },
              }, {"key_as_string": "2022-12-04T16: 30: 00.000Z", "key": 1670171400000, "doc_count": 2, "percentiles": {"values": {"50.0": 8.93768415E7, "90.0": 9.6636672E7}}}],
            },
          }, {
            "key": "GET /api/v1/timelines/public", "doc_count": 15, "by_field": {
              "buckets": [{
                "key_as_string": "2022-12-04T15: 30: 00.000Z", "key": 1670167800000, "doc_count": 3, "percentiles": {
                  "values": {
                    "50.0": 6.5626031E7, "90.0": 9.35811E7,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 00: 00.000Z", "key": 1670169600000, "doc_count": 3, "percentiles": {
                  "values": {
                    "50.0": 6.1940059E7, "90.0": 7.6175643E7,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 30: 00.000Z", "key": 1670171400000, "doc_count": 3, "percentiles": {
                  "values": {
                    "50.0": 4.8645645E7, "90.0": 9.1461477E7,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T17: 00: 00.000Z", "key": 1670173200000, "doc_count": 4, "percentiles": {
                  "values": {
                    "50.0": 1.068529325E8, "90.0": 3.33891401E8,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T17: 30: 00.000Z", "key": 1670175000000, "doc_count": 2, "percentiles": {
                  "values": {
                    "50.0": 2.222765285E8, "90.0": 4.13459701E8,
                  },
                },
              }],
            },
          }, {
            "key": "GET /api/v1/instance", "doc_count": 10, "by_field": {
              "buckets": [{
                "key_as_string": "2022-12-04T15: 30: 00.000Z", "key": 1670167800000, "doc_count": 3, "percentiles": {
                  "values": {
                    "50.0": 2.8086181E7, "90.0": 9.8721979E7,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 00: 00.000Z", "key": 1670169600000, "doc_count": 1, "percentiles": {
                  "values": {
                    "50.0": 1.07472413E8, "90.0": 1.07472413E8,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 30: 00.000Z", "key": 1670171400000, "doc_count": 3, "percentiles": {
                  "values": {
                    "50.0": 9.2505976E7, "90.0": 1.12686286E8,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T17: 00: 00.000Z", "key": 1670173200000, "doc_count": 3, "percentiles": {
                  "values": {
                    "50.0": 1.04592543E8, "90.0": 1.16033711E8,
                  },
                },
              }],
            },
          }, {
            "key": "GET /api/v1/instance/peers", "doc_count": 7, "by_field": {
              "buckets": [{
                "key_as_string": "2022-12-04T15: 30: 00.000Z", "key": 1670167800000, "doc_count": 6, "percentiles": {
                  "values": {
                    "50.0": 1.33897445E7, "90.0": 1.9978398700000003E7,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 00: 00.000Z", "key": 1670169600000, "doc_count": 0, "percentiles": {
                  "values": {
                    "50.0": null, "90.0": null,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 30: 00.000Z", "key": 1670171400000, "doc_count": 0, "percentiles": {
                  "values": {
                    "50.0": null, "90.0": null,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T17: 00: 00.000Z", "key": 1670173200000, "doc_count": 0, "percentiles": {
                  "values": {
                    "50.0": null, "90.0": null,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T17: 30: 00.000Z", "key": 1670175000000, "doc_count": 1, "percentiles": {
                  "values": {
                    "50.0": 1.4642813E7, "90.0": 1.4642813E7,
                  },
                },
              }],
            },
          }, {
            "key": "GET /", "doc_count": 6, "by_field": {
              "buckets": [{
                "key_as_string": "2022-12-04T15: 30: 00.000Z", "key": 1670167800000, "doc_count": 1, "percentiles": {
                  "values": {
                    "50.0": 6.8011157E7, "90.0": 6.8011157E7,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 00: 00.000Z", "key": 1670169600000, "doc_count": 0, "percentiles": {
                  "values": {
                    "50.0": null, "90.0": null,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 30: 00.000Z", "key": 1670171400000, "doc_count": 4, "percentiles": {
                  "values": {
                    "50.0": 3.51601935E7, "90.0": 5.4713E7,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T17: 00: 00.000Z", "key": 1670173200000, "doc_count": 1, "percentiles": {
                  "values": {
                    "50.0": 6.5643128E7, "90.0": 6.5643128E7,
                  },
                },
              }],
            },
          }, {
            "key": "GET /.well-known/webfinger(.: format)", "doc_count": 5, "by_field": {
              "buckets": [{
                "key_as_string": "2022-12-04T16: 00: 00.000Z", "key": 1670169600000, "doc_count": 2, "percentiles": {
                  "values": {
                    "50.0": 8392766.5, "90.0": 8461668.0,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T16: 30: 00.000Z", "key": 1670171400000, "doc_count": 0, "percentiles": {
                  "values": {
                    "50.0": null, "90.0": null,
                  },
                },
              }, {
                "key_as_string": "2022-12-04T17: 00: 00.000Z", "key": 1670173200000, "doc_count": 3, "percentiles": {
                  "values": {

                    "50.0": 8764120.0, "90.0": 1.3213509E7,
                  },
                },
              }],
            },
          },
        ],
      }.to_json

      # Just making sure it parses
      ES::SearchResult::TopLevelAggregatedResult.from_json(json)
    end
  end
end
