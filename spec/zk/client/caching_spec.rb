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
    expect(zkcc["/test/boom"]).to eq("goat")
    expect(zkcc["/test/foo"]).to eq(1)

    zk.create("/test/boom")
    zk.set("/test/boom", "cat")
    almost_there { expect(zkcc["/test/boom"]).to eq("cat") }

    zk.create("/test/foo")
    zk.set("/test/foo", "1")
    almost_there { expect(zkcc["/test/foo"]).to eq(2) }
  end

  describe "flakey connections", proxy: true do
    let(:host) { "#{ZK.default_host}:#{ZK.test_proxy_port}" }

    context "with slow connections", throttle_bytes_per_sec: 100 do
      it "works" do
        expect(zkcc["/test/boom"]).to eq("goat")
        expect(zkcc["/test/foo"]).to eq(1)

        zk.create("/test/boom")
        zk.set("/test/boom", "cat")
        almost_there { expect(zkcc["/test/boom"]).to eq("cat") }

        zk.create("/test/foo")
        zk.set("/test/foo", "1")
        almost_there { expect(zkcc["/test/foo"]).to eq(2) }
      end
    end

    it "works with dropped connections" do
      expect(zkcc["/test/boom"]).to eq("goat")
      zk.create("/test/boom")
      zk.set("/test/boom", "cat")
      almost_there { expect(zkcc["/test/boom"]).to eq("cat") }
      proxy_stop
      sleep 1
      zk.set("/test/boom", "dog")
      expect(zkcc["/test/boom"]).to eq("cat")
      sleep 12
      proxy_start
      sleep 1
      almost_there { expect(zkcc["/test/boom"]).to eq("dog") }
    end
  end
end
