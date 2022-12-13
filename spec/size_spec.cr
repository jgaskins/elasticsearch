require "./spec_helper"

require "../src/size"

describe ES::Size do
  it "inspects using SI prefixes" do
    ES::Size.new(1024).inspect.should eq "1.0KB"
    ES::Size.new(1024).to_s.should eq "1.0KB"
    ES::Size.new(123 * 1024).inspect.should eq "123KB"

    # 123_456_789 // 1024 // 1024 == 117
    ES::Size.new(123_456_789).inspect.should eq "117MB"
  end

  it "deserializes from JSON" do
    ES::Size.from_json("123456789").should eq ES::Size.new(123_456_789)
  end

  it "serializes into JSON" do
    ES::Size.new(123_456_789).to_json.should eq "123456789"
  end
end
