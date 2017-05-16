module ZK
  def self.spawn_zookeeper?
    !!ENV['SPAWN_ZOOKEEPER']
  end

  def self.travis?
    !!ENV['TRAVIS']
  end

  @test_port ||= spawn_zookeeper? ? (27183..28_000).detect { |port| !system("nc -z localhost #{port}") } : 2181
  @test_proxy_port ||= (@test_port + 1..28_000).detect { |port| !system("nc -z localhost #{port}") }

  class << self
    attr_accessor :test_port, :test_proxy_port
  end

  # argh, blah, this affects ZK.new everywhere (which is kind of the point, but
  # still gross)
  self.default_port = self.test_port

  # only for testing is this done
  if host = ENV['ZK_DEFAULT_HOST']
    self.default_host = host
  end
end

