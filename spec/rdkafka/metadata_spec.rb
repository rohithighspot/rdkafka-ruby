require "spec_helper"

describe Rdkafka::Producer do
  let(:metadata) do
    producer = rdkafka_config.producer
    def producer.metadata
      ::Rdkafka::Metadata.new(@native_kafka)
    end
    producer.metadata
  end

  it "can be constructed without error" do
    expect { metadata }.not_to raise_error
  end

  it "can return topic metadata" do
    topics = metadata.topics
    expect(topics).not_to be_empty
    topic_metadata = topics.first
    expect(topic_metadata.keys).to include(:topic_name)
    expect(topic_metadata.keys).to include(:partition_count)
    expect(topic_metadata.keys).to include(:partitions)
  end

  it "raises RdkafkaError if rd_kafka_metadata returns an error" do
    allow(Rdkafka::Bindings).to receive(:rd_kafka_metadata).with(any_args).and_return(42)
    expect { metadata.topics }.to raise_error(::Rdkafka::RdkafkaError)
  end
end
