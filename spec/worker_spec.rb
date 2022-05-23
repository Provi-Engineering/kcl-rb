require 'spec_helper'

RSpec.describe Kcl::Worker do
  include_context 'use_kinesis'

  let(:record_processor_factory) { double('record_processor_factory') }
  let(:worker) { Kcl::Worker.new('test-worker', record_processor_factory) }

  before do
    allow(record_processor_factory).to receive(:create_processor)
  end

  describe '#sync_shards!' do
    subject { worker.sync_shards! }
    it { expect(subject.keys.size).to eq(5) }
  end

  describe '#available_lease_shard?' do
    subject { worker.available_lease_shard? }

    context 'before consume' do
      before do
        worker.sync_shards!
      end

      it { expect(subject).to be_truthy }
    end

    context 'after consume' do
      let(:consumer) { instance_double(Kcl::Workers::Consumer) }

      before do
        allow(Kcl::Workers::Consumer).to receive(:new).and_return(consumer)
        allow(consumer).to receive(:consume!).and_return(true)

        worker.sync_shards!
        worker.consume_shards!
      end

      it { expect(subject).to be_truthy }
    end
  end

  describe '#consume_shards!' do
    let(:consumer) { instance_double(Kcl::Workers::Consumer) }

    class ExpectedWorkerError < StandardError; end

    it 'halts other threads, logging errors' do
      allow(Kcl.logger).to receive(:debug).and_call_original
      allow(Kcl::Workers::Consumer).to receive(:new).and_return(consumer)

      allow(consumer).to receive(:consume!).and_raise(ExpectedWorkerError, 'Thread encountered a problem')

      worker.sync_shards!
      expect { worker.consume_shards! }.to raise_error(ExpectedWorkerError)

      # each thread logs a problem
      expect(Kcl.logger).to have_received(:debug).with(/Killed threads due to error/).exactly(5).times

      # ensure block is run for each thread
      expect(Kcl.logger).to have_received(:debug).with(/Finish to consume shard/).exactly(5).times
    end
  end
end
