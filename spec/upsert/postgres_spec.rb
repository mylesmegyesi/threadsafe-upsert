require 'securerandom'
require 'upsert/postgres'
require 'upsert/shared_examples'

describe Upsert::Postgres do
  let(:db_name) { "upsert_postgres_test_#{Random.rand(100)}" }

  around :each do |example|
    if RUBY_PLATFORM == 'java'
      with_jdbc_database example
    else
      with_mri_database example
    end
  end

  def with_mri_database(example)
    require 'pg'
    conn = PG.connect(host: 'localhost', user: 'postgres')
    begin
      conn.exec("CREATE DATABASE #{db_name}")
      example.run
    ensure
      conn.query("DROP DATABASE #{db_name}")
      conn.close
    end
  end

  def with_jdbc_database(example)
    require 'java'
    require 'jdbc/postgres'
    Jdbc::Postgres.load_driver(:require)
    Java::org.postgresql.Driver
    conn = java.sql.DriverManager.get_connection('jdbc:postgresql://localhost/', 'postgres', nil)
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
      Sequel.connect("jdbc:postgresql://localhost/#{db_name}?user=postgres", single_threaded: true, max_connections: 1)
    else
      Sequel.connect({
        adapter: 'postgres',
        host: 'localhost',
        user: 'postgres',
        database: db_name,
        single_threaded: true,
        max_connections: 1
      })
    end
  end

  include_examples 'upsert'

end
