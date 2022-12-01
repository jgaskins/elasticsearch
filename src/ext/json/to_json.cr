# :nodoc:
struct NamedTuple
  def to_json(json : ::JSON::Builder)
    json.object { to_json_properties json }
  end

  def to_json_properties(json : ::JSON::Builder)
    {% for key in T.keys %}
      json.field {{key.stringify}} do
        self[{{key.symbolize}}].to_json(json)
      end
    {% end %}
  end
end

# :nodoc:
class Hash
  def to_json(json : ::JSON::Builder)
    json.object { to_json_properties json }
  end

  def to_json_properties(json : JSON::Builder) : Nil
    each do |key, value|
      json.field key.to_json_object_key do
        value.to_json(json)
      end
    end
  end
end
