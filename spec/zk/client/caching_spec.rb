# frozen_string_literal: true
require "spec_helper"

describe ZK::Client::Caching, zookeeper: true do
  let(:host) { "#{ZK.default_host}:#{ZK.test_port}" }
  let(:zkcc) do
    ZK::Client::Caching.new(host: host, logger: SpecGlobalLogger.logger, timeout: 10, zk_opts: {timeout: 5}) do |z|
      z.register("/test/boom", "goat")
      z.register("/test/foo", 1) { |raw_value| raw_value.to_i * 2 }
    end
  end

  let(:zk) do
    ZK.new("#{ZK.default_host}:#{ZK.test_port}")
  end

  after do
    zk.close!
    zkcc.close!
  end

  it "registers paths and listens for updates" do
    zkcc["/test/boom"].should == "goat"
    zkcc["/test/foo"].should == 1

    zk.create("/test/boom")
    zk.set("/test/boom", "cat")
    almost_there { zkcc["/test/boom"].should == "cat" }

    zk.create("/test/foo")
    zk.set("/test/foo", "1")
    almost_there { zkcc["/test/foo"].should == 2 }
  end

  describe "flakey connections", proxy: true do
    let(:host) { "#{ZK.default_host}:#{ZK.test_proxy_port}" }

    context "with slow connections", throttle_bytes_per_sec: 100 do
      it "works" do
        zkcc["/test/boom"].should == "goat"
        zkcc["/test/foo"].should == 1

        zk.create("/test/boom")
        zk.set("/test/boom", "cat")
        almost_there { zkcc["/test/boom"].should == "cat" }

        zk.create("/test/foo")
        zk.set("/test/foo", "1")
        almost_there { zkcc["/test/foo"].should == 2 }
      end
    end

    it "works with dropped connections" do
      zkcc["/test/boom"].should == "goat"
      zk.create("/test/boom")
      zk.set("/test/boom", "cat")
      almost_there { zkcc["/test/boom"].should == "cat" }
      proxy_stop
      sleep 1
      zk.set("/test/boom", "dog")
      zkcc["/test/boom"].should == "cat"
      sleep 12
      proxy_start
      sleep 1
      almost_there { zkcc["/test/boom"].should == "dog" }
    end
  end
end
