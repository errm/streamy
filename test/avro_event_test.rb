require "test_helper"
require "avro_turf/test/fake_confluent_schema_registry_server"
require "webmock/minitest"

module Streamy
  class AvroEventTest < Minitest::Test
    def setup
      Streamy.configuration.avro_schema_registry_url = "http://registry.example.com"
      Streamy.configuration.avro_schemas_path = "test/fixtures/schemas"
      FakeConfluentSchemaRegistryServer.clear
      stub_request(:any, /^#{Streamy.configuration.avro_schema_registry_url}/).to_rack(FakeConfluentSchemaRegistryServer)
    end

    class TestEvent < AvroEvent
      def topic
        :bacon
      end

      def body
        {
          smoked: "true",
          streaky: "false"
        }
      end

      def event_time
        "nowish"
      end
    end

    class IncorrectAttributeEvent < AvroEvent
      def topic
        :bacon
      end

      def body
        {
          smoked: "true",
          streaky: 100
        }
      end

      def event_time
        "nowish"
      end
    end

    class EventWithNoSchema < AvroEvent
      def topic; end
      def body; end
      def event_time; end
    end

    def test_publish
      SecureRandom.stubs(:uuid).returns("IAMUUID")

      TestEvent.publish

      assert_delivered_message(
        key: "IAMUUID",
        topic: :bacon,
        priority: :standard,
        payload: "\u0000\u0000\u0000\u0000\u0000\u0014test_event\u0002\fnowish\u0002\btrue\u0002\nfalse"
      )
    end

    def test_helpful_error_message_on_incorrect_attribute_type
      exception = assert_raises Streamy::PublicationFailedError do
        IncorrectAttributeEvent.publish
      end
      assert_match("Avro::IO::AvroTypeError", exception.message)
    end

    def test_helpful_error_message_on_event_with_no_schema
      exception = assert_raises Streamy::PublicationFailedError do
        EventWithNoSchema.publish
      end
      assert_match("AvroTurf::SchemaNotFoundError", exception.message)
    end
  end
end
