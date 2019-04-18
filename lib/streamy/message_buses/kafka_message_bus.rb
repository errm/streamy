require "streamy/kafka_configuration"
require "kafka"
require "active_support/core_ext/hash/indifferent_access"
require "active_support/json"

module Streamy
  module MessageBuses
    class KafkaMessageBus < MessageBus
      require "streamy/message_buses/kafka_message_bus/delivery"

      delegate :deliver_messages, to: :sync_producer, prefix: true

      def initialize(config)
        @config = KafkaConfiguration.new(config)
        @kafka = Kafka.new(@config.kafka)
      end

      def deliver(key:, topic:, payload:, priority:)
        Delivery.new(
          bus: self,
          priority: priority,
        ).deliver(key: key, topic: topic, payload: payload)
      end

      def shutdown
        async_producer&.shutdown
        all_sync_producers.map(&:shutdown)
      end

      #private

        attr_reader :kafka, :config

        def async_producer
          @_async_producer ||= kafka.async_producer(**config.async)
        end

        def sync_producer
          # One synchronous producer per-thread to avoid problems with concurrent deliveries.
          Thread.current[:streamy_kafka_sync_producer] ||= kafka.producer(**config.producer)
        end

        def all_sync_producers
          Thread.list.map do |thread|
            thread[:streamy_kafka_sync_producer]
          end.compact
        end

        def max_buffer_size
          config.producer[:max_buffer_size]
        end
    end
  end
end
