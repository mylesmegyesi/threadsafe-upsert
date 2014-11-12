require 'upsert/sqlite'
require 'upsert/shared_examples'
require 'tempfile'

describe Upsert::Sqlite do

  around :each do |example|
    database_file = Tempfile.new('test.db')
    @db_filepath = database_file.path
    begin
      example.run
    ensure
      database_file.close
      database_file.unlink
    end
  end

  def establish_single_connection
    config = "sqlite://#{@db_filepath}"
    config = if RUBY_PLATFORM == 'java'
               "jdbc:#{config}"
             else
               config
             end
    Sequel.connect(config, single_threaded: true, max_connections: 1)
  end

  include_examples 'upsert'

end
