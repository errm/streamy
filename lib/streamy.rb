module Streamy
  # External
  # TODO: Move into classes that use them
  require "active_support"
  require "active_support/core_ext/string"

  require "streamy/version"
  require "streamy/configuration"
  require "streamy/consumer"
  require "streamy/event_handler"
  require "streamy/message_processor"
  require "streamy/profiler"
  require "streamy/simple_logger"

  # Event types
  require "streamy/event"
  require "streamy/json_event"
  require "streamy/avro_event"

  # Errors
  require "streamy/errors/event_handler_not_found_error"
  require "streamy/errors/publication_failed_error"
  require "streamy/errors/type_not_found_error"

  # Message Buses
  require "streamy/message_buses/message_bus"

  class << self
    attr_accessor :message_bus, :logger, :dispatcher

    def shutdown
      message_bus.try(:shutdown)
    end
  end

  self.message_bus = MessageBuses::MessageBus.new
  self.logger = SimpleLogger.new
  self.dispatcher = Dispatcher

  def self.configuration
    @configuration ||= Configuration.new
  end

  def self.configure
    yield(configuration)
  end
end

at_exit { Streamy.shutdown }
