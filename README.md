# elasticsearch

Crystal client for Elasticsearch, allowing querying of documents and deserialization into Crystal objects.

NOTE: This library is usable, but a bit rough at the moment. And some things will change.

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     elasticsearch:
       github: jgaskins/elasticsearch
   ```

2. Run `shards install`

## Usage

Require the shard into your app and set up a client:

```crystal
require "elasticsearch"

# Uses ELASTICSEARCH_URL environment variable or defaults to http://localhost:9200
es = Elasticsearch::Client.new

# You can also specify an Elasticsearch URI
es = Elasticsearch::Client.new(URI.parse("https://search.example.com:9200"))

# You can also abbreviate the Elasticsearch namespace as ES
es = ES::Client.new
```

### Create an index

You can use the Elasticsearch index API directly:

```crystal
es.indices.create "products",
  mappings: ES::Mapping.new(
    properties: ES::Properties{
      "name" => ES::Property.new(:text),
      "quantity_sold" => ES::Property.new(:long),
      "description" => ES::Property.new(:text),
      "category" => ES::Property.new(:keyword),
      "created_at" => ES::Property.new(:date),
    },
  )
```

Or you can provision an index from a document model type:

```crystal
struct Product
  include ES::Serializable

  getter name : String
  getter quantity_sold : Int64
  getter description : String
  @[ES::Field(type: ES::Property.new(:keyword))]
  getter category : String
  getter created_at : Time
end

Product.provision_index "products", client: es
```

### Searching

```crystal
results = es.search("products", simple_query_string: "computers", as: Product)
results.each do |hit|
  hit        # => ES::SearchResult::Hit(Product)
  hit.source # => Product
end
```

## Development

TODO: Write development instructions here

## Contributing

1. Fork it (<https://github.com/jgaskins/elasticsearch/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Jamie Gaskins](https://github.com/jgaskins) - creator and maintainer
