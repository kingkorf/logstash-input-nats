# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "nats/client"

# .Compatibility Note
# [NOTE]
# ================================================================================
# This plugin does not support SSL authentication yet
#
# ================================================================================

# This input plugin will read events from a NATS instance; it does not support NATS streaming instance.
# This plugin used the following ruby nats client: https://github.com/nats-io/ruby-nats
#
# For more information about Nats, see <http://nats.io>
#
# Examples:
#
# [source,ruby]
#   input {
#     # Read events on subject "example" by using an "url" without authentication
#     nats {
#       servers => "nats://user:pass@localhost:4222,nats://localhost:4223"
#       subjects => ["example"]
#     }
#   }

class LogStash::Inputs::Nats < LogStash::Inputs::Base
  config_name "nats"

  milestone 1

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "json"

  # SSL
  config :ssl, :validate => :boolean, :default => false

  # Servers to connect to, for clustered usage
  config :servers, :validate => :string, :default => nil

  # List of subjects to subscribe on
  config :subjects, :validate => :array, :default => ["logstash"]

  # The queue group to join if needed
  config :queue_group, :validate => :string, :required => false

  # The name of the nats client
  config :name, :validate => :string, :required => false

  # Path of the private key file if ssl is used
  config :private_key_file, :validate => :string, :required => false

  # Path of the certificate file if ssl is used
  config :cert_file, :validate => :string, :required => false

  # Turn on ACK
  config :verbose, :validate => :boolean, :default => false

  # Turns on additional strict format checking
  config :pedantic, :validate => :boolean, :default => false

  # Time to wait before reconnecting
  config :reconnect_time_wait, :validate => :number

  # Number of attempts to connect on nats server
  config :max_reconnect_attempts, :validate => :number

  public
  def register
    @nats_config = {
      ssl: @ssl,
      name: @name,
      pedantic: @pedantic,
      verbose: @verbose,
      reconnect_time_wait: @reconnect_time_wait.nil? ? nil : @reconnect_time_wait.value,
      max_reconnect_attempts: @max_reconnect_attempts.nil? ? nil : @max_reconnect_attempts.value
    }
  end # def register


  def run(queue)
    NATS.start(@servers, @nats_config) do |nats_client|
      nats_client.on_error do |error|
        @logger.error(error)
      end
      @subjects.each do |subject|
        @logger.debug("Listening on [#{subject}]")
        nats_client.subscribe(subject, :queue => @queue_group ) do |msg, _, sub|
          @codec.decode(msg) do |event|
            decorate(event)
            event.set("nats_subject", sub)
            queue << event
          end
        end
      end
    end
  end
end # class LogStash::Inputs::Nats
