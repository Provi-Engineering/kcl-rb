require 'pry'

module KclDemo
  class DemoRecordProcessor < Kcl::RecordProcessor
    # @implement
    def after_initialize(initialization_input)
      Kcl.logger.info("Initialization at #{initialization_input}")
    end

    # @implement
    def process_records(records_input)
      Kcl.logger.debug('Processing records...')

      # レコードのリストを取得
      return if records_input.records.empty?

      # rubocop:disable Lint/Debugger
      binding.pry if ENV['DEBUG'] == '1'
      # rubocop:enable Lint/Debugger

      records_input.records.each do |record|
        Kcl.logger.debug("Record = #{record}")
      end

      # チェックポイントを記録
      last_sequence_number = records_input.records[-1].sequence_number
      Kcl.logger.debug(
        "Checkpoint progress at: #{last_sequence_number}" \
        ", MillisBehindLatest = #{records_input.millis_behind_latest}"
      )
      records_input.record_checkpointer.update_checkpoint(last_sequence_number)
    end

    # @implement
    def shutdown(shutdown_input)
      Kcl.logger.debug("Shutdown reason: #{shutdown_input.shutdown_reason}")

      if shutdown_input.shutdown_reason == Kcl::Workers::ShutdownReason::TERMINATE
        shutdown_input.record_checkpointer.update_checkpoint(nil)
      end
    end
  end
end
