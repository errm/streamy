module Streamy
  class MessageBuses::KafkaMessageBus::Delivery
    ASAP_DELIVERY = %i(standard essential)
    ASYNC_DELIVERY = %i(standard low)
    BATCHED_DELIVERY = %i(batched)

    delegate :sync_producer, :async_producer, :max_buffer_size, to: :bus

    def initialize(bus:, priority:)
      @bus = bus
      @priority = priority
    end

    def deliver(key:, topic:, payload:)
      producer.produce(payload, key: key, topic: topic)

      if asap_delivery?
        perform_deliveries
      elsif batched_delivery?
        manage_batched_deliveries
      else
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

      def asap_delivery?
        ASAP_DELIVERY.include?(priority)
      end

      def batched_delivery?
        BATCHED_DELIVERY.include?(priority)
      end

      def async_delivery?
        ASYNC_DELIVERY.include?(priority)
      end

      def producer
        @_producer ||= fetch_producer
      end

      def fetch_producer
        if async_delivery?
          async_producer
        else
          sync_producer
        end
      end

      def logger
        Streamy.logger
      end
  end
end
