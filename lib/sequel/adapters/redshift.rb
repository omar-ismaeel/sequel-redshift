require 'sequel/adapters/postgres'
require 'sequel/adapters/shared/redshift'

module Sequel
  module Redshift
    include Postgres

    class Database < Postgres::Database
      include Sequel::Redshift::DatabaseMethods

      set_adapter_scheme :redshift

      DIST_KEY = ' DISTKEY'.freeze
      SORT_KEY = ' SORTKEY'.freeze

      # The order of column modifiers to use when defining a column.
      COLUMN_DEFINITION_ORDER = [:collate, :default, :primary_key, :dist_key, :sort_key, :null, :unique, :auto_increment, :references]

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

      # Add DISTKEY SQL fragment to column creation SQL.
      def column_definition_dist_key_sql(sql, column)
        if column[:dist_key]
          sql << DIST_KEY
        end
      end

      # Add SORTKEY SQL fragment to column creation SQL.
      def column_definition_sort_key_sql(sql, column)
        if column[:sort_key]
          sql << SORT_KEY
        end
      end

      def serial_primary_key_options
        # redshift doesn't support serial type
        super.merge(serial: false)
      end

      # DROP TABLE IF EXISTS is not supported by Redshift
      def supports_drop_table_if_exists?
        false
      end
    end

    def connect
      @connection = PG::Connection.connect(@connection_parameters)
      binding.pry
      configure_connection
    rescue ::PG::Error => error
      if error.message.include?("does not exist")
        raise ActiveRecord::NoDatabaseError.new(error.message, error)
      else
        raise
      end
    end

    class Dataset < Postgres::Dataset
      Database::DatasetClass = self

      def initialize(*args)
        super(*args)
        @opts = @opts.merge(:disable_insert_returning => true).freeze
      end

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
