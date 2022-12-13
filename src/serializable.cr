require "./indices"

module Elasticsearch
  annotation Field
  end

  module Serializable
    private DEFAULT_MAPPINGS = {} of String => ES::Property

    macro register_mapping(crystal_type, es_type)
      {% DEFAULT_MAPPINGS[crystal_type.stringify] = es_type %}
    end

    register_mapping UUID, ES::Property.new(:keyword)
    register_mapping String, ES::Property.new(:text)
    register_mapping Int32, ES::Property.new(:long)
    register_mapping Int64, ES::Property.new(:long)
    register_mapping Time, ES::Property.new(:date)
    register_mapping Float32, ES::Property.new(:float)
    register_mapping Float64, ES::Property.new(:double)
    register_mapping Bool, ES::Property.new(:boolean)

    macro included
      {% verbatim do %}
        include JSON::Serializable

        def self.provision_index(name : String, client : ::ES::Client, analyzer = nil, filter : ::ES::Indices::Settings::Analysis::FilterMap? = nil)
          {% begin %}
            {% properties = {} of Nil => Nil %}
            {% for ivar in @type.instance_vars %}
              {% ann = ivar.annotation(::Elasticsearch::Field) %}
              {% unless ann && (ann[:ignore] || ann[:ignore_deserialize]) %}
                {%
                  properties[ivar.id] = {
                    type:      ivar.type,
                    es_type:   (ann && ann[:type]),
                    key:       ((ann && ann[:key]) || ivar).id.stringify,
                    nilable:   ivar.type.nilable?,
                    converter: ann && ann[:converter],
                    presence:  ann && ann[:presence],
                  }
                %}
              {% end %}
            {% end %}

            if analyzer
              analysis = ES::Indices::Settings::Analysis.new(
                analyzer: analyzer,
                filter: filter,
              )
            end

            client.indices.create name,
              mappings: ES::Mapping.new(
                properties: ES::Properties{
                  {% for name, ivar in properties %}
                    {% if ivar[:nilable] %}
                      {{name.stringify}} => {{ivar[:es_type] || DEFAULT_MAPPINGS[ivar[:type].union_types.reject(&.nilable?).first.stringify]}} || ::ES::Property.new(:keyword),
                    {% else %}
                      {{name.stringify}} => {{ivar[:es_type] || DEFAULT_MAPPINGS[ivar[:type].stringify]}} || ::ES::Property.new(:keyword),
                    {% end %}
                  {% end %}
                },
              ),
              settings: ES::Indices::Settings.new(
                index: ES::Indices::Settings::Index.new(
                  analysis: analysis,
                ),
              )
          {% end %}
        end
      {% end %}
    end

    def to_json(json : ::JSON::Builder)
      json.object do
        to_json_properties(json)
      end
    end

    def to_json_properties(json : ::JSON::Builder)
      {% begin %}
        {% options = @type.annotation(::JSON::Serializable::Options) %}
        {% emit_nulls = options && options[:emit_nulls] %}

        {% properties = {} of Nil => Nil %}
        {% for ivar in @type.instance_vars %}
          {% ann = ivar.annotation(::JSON::Field) %}
          {% unless ann && (ann[:ignore] || ann[:ignore_serialize]) %}
            {%
              properties[ivar.id] = {
                type:      ivar.type,
                key:       ((ann && ann[:key]) || ivar).id.stringify,
                root:      ann && ann[:root],
                converter: ann && ann[:converter],
                emit_null: (ann && (ann[:emit_null] != nil) ? ann[:emit_null] : emit_nulls),
              }
            %}
          {% end %}
        {% end %}

        {% for name, value in properties %}
          _{{name}} = @{{name}}

          {% unless value[:emit_null] %}
            unless _{{name}}.nil?
          {% end %}

            json.field({{value[:key]}}) do
              {% if value[:root] %}
                {% if value[:emit_null] %}
                  if _{{name}}.nil?
                    nil.to_json(json)
                  else
                {% end %}

                json.object do
                  json.field({{value[:root]}}) do
              {% end %}

              {% if value[:converter] %}
                if _{{name}}
                  {{ value[:converter] }}.to_json(_{{name}}, json)
                else
                  nil.to_json(json)
                end
              {% else %}
                _{{name}}.to_json(json)
              {% end %}

              {% if value[:root] %}
                {% if value[:emit_null] %}
                  end
                {% end %}
                  end
                end
              {% end %}
            end

          {% unless value[:emit_null] %}
            end
          {% end %}
        {% end %}
        on_to_json(json)
      {% end %}
    end
  end
end
