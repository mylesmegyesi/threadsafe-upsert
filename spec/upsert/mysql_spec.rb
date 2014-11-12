require 'securerandom'
require 'upsert/mysql'
require 'upsert/shared_examples'

describe Upsert::Mysql do
  let(:db_name) { "upsert_mysql_test_#{Random.rand(100)}" }

  around :each do |example|
    if RUBY_PLATFORM == 'java'
      with_jdbc_database example
    else
      with_mri_database example
    end
  end

  def with_mri_database(example)
    require 'mysql2'
    client = Mysql2::Client.new(host: 'localhost', username: 'root')
    begin
      client.query("CREATE DATABASE #{db_name}")
      example.run
    ensure
      client.query("DROP DATABASE #{db_name}")
      client.close
    end
  end

  def with_jdbc_database(example)
    require 'java'
    require 'jdbc/mysql'
    Jdbc::MySQL.load_driver(:require)
    Java::com.mysql.jdbc.Driver
    conn = java.sql.DriverManager.get_connection('jdbc:mysql://localhost/', 'root', nil)
    begin
      conn.create_statement.execute_update("CREATE DATABASE #{db_name}")
      example.run
    ensure
      conn.create_statement.execute_update("DROP DATABASE #{db_name}")
      conn.close
    end
  end

  def establish_single_connection
    if RUBY_PLATFORM == 'java'
      Sequel.connect("jdbc:mysql://localhost/#{db_name}?user=root", single_threaded: true, max_connections: 1)
    else
      Sequel.connect({
        adapter: 'mysql2',
        host: 'localhost',
        user: 'root',
        database: db_name,
        single_threaded: true,
        max_connections: 1
      })
    end
  end

  include_examples 'upsert'

end
