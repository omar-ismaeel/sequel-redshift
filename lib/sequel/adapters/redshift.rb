require 'sequel/adapters/postgres'
require 'sequel/adapters/shared/redshift'

module Sequel
  module Redshift
    include Postgres

    class Database < Postgres::Database
      include Sequel::Redshift::DatabaseMethods

      set_adapter_scheme :redshift

      # We need to change these default settings because they correspond to
      # Postgres configuration variables which do not exist in Redshift
      def adapter_initialize
        @opts.merge!(
          force_standard_strings: false,
          client_min_messages:    false
        )
        super
      end

      def column_definition_primary_key_sql(sql, column)
        result = super
        result << ' IDENTITY' if result
        result
      end

      def serial_primary_key_options
        # redshift doesn't support serial type
        super.merge(serial: false)
      end
    end

    class Dataset < Postgres::Dataset
      Database::DatasetClass = self

      def insert_returning_sql(sql)
        sql
      end

      def supports_returning?(type)
        false
      end

      def supports_insert_select?
        false
      end
    end
  end
end
