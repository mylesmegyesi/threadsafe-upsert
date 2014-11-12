require 'connection_pool'
require 'concurrent'
require 'thread_safe'

module Upsert
  module Helpers

    def upsert(connection, vendor_name, vendor_email, vendor_updated_at)
      described_class.upsert(connection, vendor_name, vendor_email, vendor_updated_at)
    end

    def with_db_connection(&block)
      connection = establish_single_connection
      begin
        block.call(connection)
      ensure
        connection.disconnect
      end
    end

    def with_db_connection_pool(size, &block)
      pool = ConnectionPool.new(size: size, timeout: 0) { establish_single_connection }
      begin
        block.call(pool)
      ensure
        pool.shutdown { |conn| conn.disconnect }
      end
    end

    def with_thread_pool(size, &block)
      pool = Concurrent::FixedThreadPool.new(size)
      begin
        block.call(pool)
      ensure
        pool.shutdown
        pool.wait_for_termination
      end
    end

    def in_parallel(options, &block)
      results = ThreadSafe::Array.new
      with_db_connection_pool(options[:concurrency]) do |connection_pool|
        with_thread_pool(options[:concurrency]) do |thread_pool|
          args = if args = options[:args]
                   args
                 else
                   (1..options[:times]).map do |i|
                     nil
                   end
                 end
          args.each_with_index do |arg, i|
            thread_pool.post(arg) do |arg|
              connection_pool.with do |connection|
                results << block.call(connection, arg)
              end
            end
          end
        end
      end
      results
    end

    def all_people(connection)
      utc = ActiveSupport::TimeZone['UTC']
      connection.from(:people).map do |person|
        vendor_updated_at = person[:vendor_updated_at]
        if vendor_updated_at.is_a?(String)
          person[:vendor_updated_at] = utc.parse(vendor_updated_at)
        end
        person
      end
    end

    def generate_n_times_between(start, _end, total)
      start_i = start.to_i
      end_i = _end.to_i
      step = (end_i - start_i) / total
      (start_i..end_i).step(step).map do |i|
        Time.at(i).utc
      end.take(total)
    end

  end
end
