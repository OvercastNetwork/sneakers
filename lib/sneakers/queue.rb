
class Sneakers::Queue
  attr_reader :name, :opts, :exchange

  def initialize(name, opts)
    @name = name
    @opts = opts
    @handler_klass = Sneakers::CONFIG[:handler]
  end

  def subscribe(worker)
    @channel = Sneakers.bunny.create_channel
    @channel.prefetch(@opts[:prefetch])

    # TODO: get the arguments from the handler? Retry handler wants this so you
    # don't have to line up the queue's dead letter argument with the exchange
    # you'll create for retry.
    queue_durable = @opts[:queue_durable].nil? ? @opts[:durable] : @opts[:queue_durable]
    @queue = @channel.queue(@name, :durable => queue_durable, :arguments => @opts[:arguments])

    @exchange = bind(@opts[:exchange])

    # NOTE: we are using the worker's options. This is necessary so the handler
    # has the same configuration as the worker. Also pass along the exchange and
    # queue in case the handler requires access to them (for things like binding
    # retry queues, etc).
    handler_klass = worker.opts[:handler] || Sneakers::CONFIG[:handler]
    handler = handler_klass.new(@channel, @queue, worker.opts)

    @consumer = @queue.subscribe(:block => false, :manual_ack => @opts[:ack]) do | delivery_info, metadata, msg |
      worker.do_work(delivery_info, metadata, msg, handler)
    end
    nil
  end

  def unsubscribe
    @consumer.cancel if @consumer
    @consumer = nil

    @channel.close if @channel && !@channel.closed?
    @channel = nil
  end

    def bind(name, **opts)
        exchange = @channel.exchange(name,
                                     :type => opts[:type] || @opts[:exchange_type],
                                     :durable => opts[:durable] || @opts[:exchange_durable])
        @queue.bind(exchange, :routing_key => opts[:routing_key] || @opts[:routing_key] || @name)
        exchange
    end
end
