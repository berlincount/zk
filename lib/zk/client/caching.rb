# frozen_string_literal: true
require "concurrent"
require "forwardable"

module ZK
  module Client
    class Caching
      extend Forwardable

      def_delegator(:@cache, :[])

      attr_reader :exceptions

      def initialize(host:, logger: nil, timeout: 30, zk_opts: {}, zk: nil)
        @host = host
        @logger = logger
        @cache = Concurrent::Map.new
        @watches = Concurrent::Map.new
        @default_values = {}
        @latch = Zookeeper::Latch.new
        @new_session = true
        @missed_updates = Concurrent::Hash.new
        @exceptions = Set.new
        @registerable = true

        if block_given? && !zk
          yield(self)
          @default_values.freeze

          expiration = Time.now + timeout
          @zk = connect(zk_opts)

          wait_for_warm_cache(expiration - Time.now)
        elsif !block_given? && zk
          @zk = zk
        else
          raise ArgumentError, "must pass in a ZK::Client or a block"
        end
      end

      def register(path, default_value, &block)
        raise ArgumentError, "register only allowed before setup_callbacks called" unless @registerable

        debug("added path=#{path} default_value=#{default_value.inspect}")
        @cache[path] = default_value
        @default_values[path] = Value.new(default_value, block)
      end

      def setup_callbacks(zk)
        @registerable = false
        raise ArgumentError, "the ZK::Client can't be connected" if zk.connected? || zk.connecting?

        zk.on_connected do |e|
          info("on_connected new_session=#{@new_session} #{e.event_name} #{e.state_name}")
          next unless @new_session

          @missed_updates.clear
          @default_values.each do |path, _value|
            @watches[path] ||= zk.register(path) do |event|
              @missed_updates.reject! do |missed_path, _|
                info("update_cache with previously missed path=#{missed_path}")
                update_cache(zk, missed_path)
              end
              if event.node_event?
                debug("node event=#{event.inspect} #{event.event_name} #{event.state_name}")
                @missed_updates[path] = true unless update_cache(zk, event.path)
              else
                warn("session event=#{event.inspect}")
              end
            end
            @missed_updates[path] = true unless update_cache(zk, path)
          end
          @new_session = false
          @latch.release if @latch
        end

        zk.on_expired_session do |e|
          info("on_expired_session #{e.event_name} #{e.state_name}")
          @new_session = true
        end

        zk.on_exception do |e|
          error("on_exception exception=#{e.inspect} backtrace=#{e.backtrace.inspect}")
        end
      end

      def wait_for_warm_cache(timeout = 30)
        warn("didn't connect before timeout") unless @zk.connected? && timeout > 0 && @latch.await(timeout)
        @latch = nil
      end

      def close!
        @watches.each_value(&:unsubscribe)
        @watches.clear
        @zk.close!
      end

      private

      def connect(zk_opts)
        raise ArgumentError, "already connected" if @zk

        debug("connecting host=#{@host.inspect}")
        ZK.new(@host, **zk_opts) do |zk|
          setup_callbacks(zk)
        end
      end

      # only called from ZK thread
      def update_cache(zk, path)
        debug("update_cache path=#{path}")

        unless zk.exists?(path, watch: true)
          @cache[path] = @default_values[path].default_value
          debug("no node, setting watch path=#{path}")
          return true
        end

        raw_value, stat = zk.get(path, watch: true)
        value = @default_values[path].deserialize(raw_value, stat)
        @cache[path] = value

        debug("updated cache path=#{path} raw_value=#{raw_value.inspect} value=#{value.inspect}")
        true
      rescue ::ZK::Exceptions::ZKError => e
        exceptions << e.class
        warn("update_cache path=#{path} exception=#{e.inspect}, retrying")
        retry
      rescue ::ZK::Exceptions::KeeperException, ::Zookeeper::Exceptions::ZookeeperException => e
        exceptions << e.class
        warn("update_cache path=#{path} exception=#{e.inspect}")
        warn(
          "zk=#{zk.inspect} closed=#{zk.closed?} connected?=#{zk.connected?} connecting?=#{zk.connecting?} " \
          "expired_session?=#{zk.expired_session?}"
        )
        false
      end

      %w(debug info warn error).each do |m|
        module_eval <<~EOM, __FILE__, __LINE__
      def #{m}(msg)
        return unless @logger
        @logger.#{m}("ZkCachingClient") { msg }
      end
        EOM
      end

      class Value < Struct.new(:default_value, :deserializer)
        def deserialize(raw, stat = nil)
          return raw unless deserializer
          deserializer.call(raw, stat)
        end
      end
    end
  end
end
