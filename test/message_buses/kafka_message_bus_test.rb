require "test_helper"

class FakeKafka
  attr_reader :messages

  def initialize
    @messages = {}
  end

  def deliver_message(payload, key:, topic:)
    messages[key] = [topic, payload]
  end
end

module Streamy
  class KafkaMessageBusTest < Minitest::Test
    def test_deliver
      fake_kafka = FakeKafka.new
      bus = MessageBuses::KafkaMessageBus.new(kafka: fake_kafka)
      bus.deliver(key: "key", topic: "topic", type: "type", body: "body", event_time: "2018")

      assert_equal fake_kafka.messages, "key" => ["topic", { type: "type", body: "body", event_time: "2018" }]
    end
  end
end
