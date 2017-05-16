require 'rubygems'
require 'bundler/setup'

# not a constant so we don't pollute global namespace
release_ops_path = File.expand_path('../../releaseops/lib', __FILE__)

if File.exists?(release_ops_path)
  require File.join(release_ops_path, 'releaseops')
  ReleaseOps::SimpleCov.maybe_start
end

Bundler.require(:development, :test)

require 'zk'
require 'benchmark'
require 'pry'

# Requires supporting ruby files with custom matchers and macros, etc,
# in spec/support/ and its subdirectories.
Dir[File.expand_path("../{support,shared}/**/*.rb", __FILE__)].sort.each {|f| require f}

$stderr.sync = true

RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  [WaitWatchers, SpecGlobalLogger, Pendings].each do |mod|
    config.include(mod)
    config.extend(mod)
  end

  if ZK.rubinius?
    config.filter_run_excluding :rbx => :broken
  end

  if ZK.mri_187?
    config.filter_run_excluding :mri_187 => :broken
  end

  if ZK.jruby?
    config.filter_run_excluding :fork_required => true
    config.filter_run_excluding :jruby => :broken
  end

  if ZK.spawn_zookeeper?
    require 'zk-server'

    config.before(:suite) do
      SpecGlobalLogger.logger.debug { "Starting zookeeper service" }
      ZK::Server.run do |c|
        c.client_port         = ZK.test_port
        c.force_sync          = false
        c.snap_count          = 1_000_000
        c.max_session_timeout = 5_000 # ms
      end
    end

    config.after(:suite) do
      SpecGlobalLogger.logger.debug  { "stopping zookeeper service" }
      ZK::Server.shutdown
    end

    config.before(:each, zookeeper: true) do
      ZK.open("#{ZK.default_host}:#{ZK.test_port}") do |zk|
        zk.rm_rf("/test")
        zk.mkdir_p("/test")
      end
    end

    config.before(:each, proxy: true) do |group|
      proxy_start(group.metadata[:throttle_bytes_per_sec])
    end

    config.after(:each, proxy: true) do
      proxy_stop
    end

    def proxy_start(throttle_bytes_per_sec = nil)
      limit = "-L #{throttle_bytes_per_sec}" if throttle_bytes_per_sec
      spawn(%{socat -T 10 -d TCP-LISTEN:#{ZK.test_proxy_port},fork,reuseaddr,linger=1 SYSTEM:'pv -q #{limit} - | socat - "TCP:localhost:#{ZK.test_port}"'})
    end

    def proxy_stop
      system("lsof -i TCP:#{ZK.test_proxy_port} -t | grep -v #{Process.pid} | xargs kill -9")
    end

    def spawn(cmd)
      puts "+ #{cmd}" if ENV['ZK_DEBUG']
      Kernel.spawn(cmd)
    end

    def system(cmd)
      puts "+ #{cmd}" if ENV['ZK_DEBUG']
      Kernel.system(cmd)
    end

    def almost_there(tries = 100)
      i ||= 0
      yield
    rescue RSpec::Expectations::ExpectationNotMetError
      raise if i > tries
      sleep 0.1
      i += 1
      retry
    end
  end

  # tester should return true if the object is a leak
  def leak_check(klass, &tester)
    count = 0
    ObjectSpace.each_object(klass) { |o| count += 1 if tester.call(o) }
    unless count.zero?
      raise "There were #{count} leaked #{klass} objects after #{example.full_description.inspect}"
    end
  end

  # these make tests run slow
  if ENV['ZK_LEAK_CHECK']
    config.after do
      leak_check(ZK::Client::Threaded) { |o| !o.closed? }
      leak_check(ZK::ThreadedCallback, &:alive?)
      leak_check(ZK::Threadpool, &:alive?)
      leak_check(Thread) { |th| Thread.current != th && th.alive? }
      expect(ZK::ForkHook.hooks.values.flatten).to be_empty
    end
  end
end

class ::Thread
  # join with thread until given block is true, the thread joins successfully,
  # or timeout seconds have passed
  #
  def join_until(timeout=2)
    time_to_stop = Time.now + timeout

    until yield
      break if Time.now > time_to_stop
      break if join(0)
      Thread.pass
    end
  end

  def join_while(timeout=2)
    time_to_stop = Time.now + timeout

    while yield
      break if Time.now > time_to_stop
      break if join(0)
      Thread.pass
    end
  end
end

if RUBY_VERSION == '1.9.3'
  Thread.current[:name] = 'main'

  trap('USR1') do
    threads = Thread.list.map { |th| { :inspect => th.inspect, :name => th[:name], :calback => th[:callback], :backtrace => th.backtrace } }
    pp threads
  end
end

