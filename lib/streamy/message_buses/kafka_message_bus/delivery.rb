module Streamy
  class MessageBuses::KafkaMessageBus::Delivery
    def initialize(producer:, timing:)
      @producer = producer
      @timing = timing
    end

    def deliver(key:, topic:, payload:)
      producer.produce(payload, key: key, topic: topic)

      case timing
      when :asap
        perform_deliveries
      when :batched
        manage_batched_deliveries
      when :later
        # leave to kafka to decide when to deliver
      end
    end

    private

      attr_reader :bus, :priority

      def perform_deliveries
        producer.deliver_messages
      end

      def manage_batched_deliveries
        if buffer_full?
          logger.info "Delivering #{producer.buffer_size} batched events now"
          perform_deliveries
        end
      end

      def buffer_full?
        producer.buffer_size >= max_buffer_size
      end

      def logger
        Streamy.logger
      end
  end
end
