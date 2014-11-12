module Upsert
  class Postgres
    def self.upsert(connection, vendor_name, vendor_email, vendor_updated_at)
      table = connection.from(:people)
      begin
        table.insert({
          vendor_name: vendor_name,
          vendor_email: vendor_email,
          vendor_updated_at: vendor_updated_at
        })
        {status: :success}
      rescue Sequel::UniqueConstraintViolation => e
        affected_rows = table.
          where(Sequel.expr(vendor_updated_at) > :vendor_updated_at).
          where(vendor_email: vendor_email).
          update({
            vendor_name: vendor_name,
            vendor_updated_at: vendor_updated_at,
          })
        if affected_rows == 0
          {status: :failure, reason: :stale_data}
        else
          {status: :success}
        end
      end
    end
  end
end
