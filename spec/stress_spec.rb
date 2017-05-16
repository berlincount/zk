# frozen_string_literal: true
require "spec_helper"

describe "stress test", zookeeper: true, proxy: true, stress: true do
  let!(:zkcc) do
    cache = ZK::Recipes::Cache.new(logger: SpecGlobalLogger.logger)
    cache.register("/test/boom", "boom") do |raw_value, stat|
      boom_versions << stat.version
      delays << Time.now - stat.mtime_t
      raw_value
    end
    cache.register("/test/foo", "foo") do |raw_value, stat|
      foo_versions << stat.version
      delays << Time.now - stat.mtime_t
      raw_value
    end
    cache.setup_callbacks(zk_cache)
    zk_cache.connect
    cache.wait_for_warm_cache(timeout)
    cache
  end

  let(:zk_cache) do
    ZK.new("#{ZK.default_host}:#{ZK.test_proxy_port}", connect: false, timeout: timeout).tap do |zk|
      zk.on_exception { |e| exceptions << e.class }
    end
  end

  let(:zk) do
    ZK.new("#{ZK.default_host}:#{ZK.test_port}")
  end

  let(:delays) { [] }
  let(:boom_versions) { [] }
  let(:foo_versions) { [] }
  let(:seconds) { 120 }
  let(:timeout) { 5 }
  let(:exceptions) { Set.new }

  after do
    zk.close!
    zkcc.close!
  end

  def update_values
    @expected_version ||= 0
    @expected_version += 1
    zk.set("/test/boom", "boom")
    zk.set("/test/foo", "foo")
    sleep(rand(0..0.01))
  end

  def slow_proxy
    proxy_stop
    proxy_start(100)
    sleep(rand(0..timeout + 1))
  end

  def expire_session
    proxy_stop
    sleep(rand(timeout + 1..35))
    proxy_start
    sleep(rand(0..timeout + 1))
  end

  it "works" do
    stop = Time.now + seconds
    threads = Array.new(5) do
      Thread.new do
        i = 0
        loop do
          zkcc["/test/boom"]
          zkcc["/test/foo"]
          i += 1
          if i % 1_000 == 0
            break if Time.now > stop
            Thread.pass if RUBY_ENGINE == "ruby"
          end
        end
        i
      end
    end

    sleep(1)
    zk.create("/test/boom", "boom")
    zk.create("/test/foo", "foo")

    until Time.now > stop
      puts "update_values" if ENV['ZK_DEBUG']
      10.times { update_values }
      puts "slow proxy" if ENV['ZK_DEBUG']
      slow_proxy
      puts "update_values" if ENV['ZK_DEBUG']
      10.times { update_values }
      puts "expire_session" if ENV['ZK_DEBUG']
      expire_session
    end

    mean = delays.reduce(:+) / delays.count
    puts "Stress Test Summary"
    puts "delay mean=#{mean.round(3)}s max=#{delays.max}s min=#{delays.min}s"
    puts "read/ms=" + threads.map { |t| t.join.value / seconds / 1_000 }.inspect
    puts "last_expected_version=#{@expected_version}"
    puts "boom_versions=#{boom_versions.inspect}"
    puts "foo_versions=#{foo_versions.inspect}"
    puts "exceptions=#{exceptions.inspect}"
  end
end
