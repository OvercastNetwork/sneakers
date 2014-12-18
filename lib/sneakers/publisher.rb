module Sneakers
  class Publisher
    def initialize(opts = {})
      @mutex = Mutex.new
      @opts = Sneakers::CONFIG.merge(opts)
    end

    def publish(msg, options = {})
      @mutex.synchronize do
        connect! unless connected?
      end
      to_queue = options.delete(:to_queue)
      options[:routing_key] ||= to_queue || @opts[:routing_key]
      Sneakers.logger.info {"publishing <#{msg}> to [#{options[:routing_key]}]"}
      @exchange.publish(msg, options)
    end

    private

    attr_reader :exchange

    def connect!
      @channel = Sneakers.bunny.create_channel
      @exchange = @channel.exchange(@opts[:exchange], type: @opts[:exchange_type], durable: @opts[:exchange_durable])
    end

    def connected?
      @channel && @channel.connected?
    end
  end
end

