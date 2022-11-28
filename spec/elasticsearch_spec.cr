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
end
