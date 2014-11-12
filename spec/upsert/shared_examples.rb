require 'active_support/all'
require 'rspec/collection_matchers'
require 'sequel'
require 'upsert/helpers'

Sequel.application_timezone = :utc
Sequel.default_timezone = :utc
Sequel.database_timezone = :utc
Sequel.typecast_timezone = :utc

shared_examples_for 'upsert' do
  include Upsert::Helpers

  let(:yesterday) { 1.day.ago(now) }
  let(:now) { Time.parse(Time.now.utc.to_s) }

  before :each do
    with_db_connection do |connection|
      connection.create_table(:people) do
        primary_key :id
        String :vendor_name
        String :vendor_email, unique: true
        Time :vendor_updated_at
      end
    end
  end

  it 'creates a person if they do not exist' do
    with_db_connection do |connection|
      result = upsert(connection, 'John', 'j@j.com', yesterday)

      expect(result).to eq(status: :success)

      people = all_people(connection)
      expect(people.size).to eq(1)

      row = people.first
      expect(row[:vendor_name]).to eq('John')
      expect(row[:vendor_email]).to eq('j@j.com')
      expect(row[:vendor_updated_at]).to eq(yesterday)
    end
  end

  it "updates the person's name and updated_at if the email already exists" do
    with_db_connection do |connection|
      create_result = upsert(connection, 'John', 'j@j.com', yesterday)
      update_result = upsert(connection, 'Johnathon', 'j@j.com', now)

      expect(update_result).to eq(status: :success)

      people = all_people(connection)
      expect(people.size).to eq(1)

      row = people.first
      expect(row[:vendor_name]).to eq('Johnathon')
      expect(row[:vendor_email]).to eq('j@j.com')
      expect(row[:vendor_updated_at]).to eq(now)
    end
  end

  it 'does not override newer data with old data' do
    with_db_connection do |connection|
      create_result = upsert(connection, 'John',      'j@j.com', now)
      update_result = upsert(connection, 'Johnathon', 'j@j.com', yesterday)

      expect(update_result).to eq({
        status: :failure,
        reason: :stale_data
      })

      people = all_people(connection)
      expect(people.size).to eq(1)
      row = people.first
      expect(row[:vendor_name]).to eq('John')
      expect(row[:vendor_email]).to eq('j@j.com')
      expect(row[:vendor_updated_at]).to eq(now)
    end
  end

  it 'handles many writers trying to update' do
    writers = 100
    updated_at_times = generate_n_times_between(yesterday, now, writers)
    in_parallel_options = {
      times: writers,
      concurrency: writers / 5,
      args: updated_at_times.shuffle
    }
    results = in_parallel(in_parallel_options) do |connection, updated_at|
      upsert(connection, 'John', 'j@j.com', updated_at)
    end

    expect(results).to have(writers).items
    successful_writes, failed_writes = results.partition do |result|
      result[:status] == :success
    end
    expect(successful_writes).to have_at_least(1).item

    with_db_connection do |connection|
      people = all_people(connection)
      expect(people.size).to eq(1)
      row = people.first
      expect(row[:vendor_name]).to eq('John')
      expect(row[:vendor_email]).to eq('j@j.com')
      expect(row[:vendor_updated_at]).to eq(updated_at_times.max)
    end
  end

  it 'handles many writers trying to insert the same piece of data' do
    writers = 100
    results = in_parallel(times: writers, concurrency: writers / 5) do |connection|
      begin
        upsert(connection, 'John', 'j@j.com', now)
      rescue => e
        puts "#{Thread.current.object_id} #{e.message}"
        raise
      end
    end

    expect(results.size).to eq(writers)
    successful_writes, failed_writes = results.partition do |result|
      result[:status] == :success
    end

    expect(successful_writes.size).to eq(1)
    expect(failed_writes.size).to eq(writers - 1)
    expect(failed_writes.map{|r| r[:reason]}).to all(eq(:stale_data))

    with_db_connection do |connection|
      people = all_people(connection)
      expect(people.size).to eq(1)
      row = people.first
      expect(row[:vendor_name]).to eq('John')
      expect(row[:vendor_email]).to eq('j@j.com')
      expect(row[:vendor_updated_at]).to eq(now)
    end
  end

end
