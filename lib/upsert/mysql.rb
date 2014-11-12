module Upsert
  class Mysql
    def self.upsert(connection, name, email, updated_at)
      table = connection.from(:people)
      begin
        table.insert({
          vendor_name: name,
          vendor_email: email,
          vendor_updated_at: updated_at
        })
        {status: :success}
      rescue Sequel::UniqueConstraintViolation => e
        affected_rows = table.
          where(Sequel.expr(updated_at) > :vendor_updated_at).
          where(vendor_email: email).
          update({
            vendor_name: name,
            vendor_updated_at: updated_at,
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
