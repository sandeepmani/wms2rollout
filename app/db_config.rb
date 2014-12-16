class DBConfig

  require "mysql2"
  Mysql2::Client.default_query_options.merge!(:as => :array)

  attr_accessor :client, :db_details

  def initialize
    self.db_details={
        :host => "localhost",
        :username => "root",
        :database => "warehouse_b2b_development",

    }
    self.client = Mysql2::Client.new(db_details)
  end
end