##########################################   Base Config ###########################################

require "mysql2"
Mysql2::Client.default_query_options.merge!(:as => :array)

class DBConfig
  attr_accessor :client, :db_details
  def initialize
    # batch_size=100
    self.db_details={
        :host => "localhost",
        :username => "root",
        :database => "warehouse_b2b_development",

    }
    self.client = Mysql2::Client.new(db_details)
  end

  MIGRATION_RULES = {
      :product_masters => {
          :base_query => {
              :query => "select fsn, sku from product_details group by seller_id",
              # haven't created query builder (from below params) using direct query for now
              :tables => ["product_details"],
              :select => "fsn, sku",
              :joins => "",
              :additional =>""


          },
          :options => {
              :direct_insert => false,
              :batch_type => "primary_key",

          },
          :map => {
              :id => :auto_increament,
              :fsn => "fsn",
              :sku => "sku",
              :status => ["status", lambda {|text| text.to_s },lambda {|str| str.to_s }],
              :created_at => :current_time,
              :updated_at => :current_time,

          },

      },

  }


end


##########################################   TargetTable ###########################################


class TargetTable
  attr_accessor :target_table, :total_count, :errors, :current_batch, :completed_count, :limit, :rule
  def initialize(name)
    self.db = DBConfig.new()
    self.name=name
    self.fields = :fwfwf
    self.rule = self.migration_rules[target_table]
  end

  def get_fields_in_order
    # puts db_client.query("show fields from #{table_name}").each{|c| puts c[1]}
    db.client.query("show fields from #{name}").collect { |c| c[0].to_sym }
  end

  def embed_string_in_quotes(arr)
    arr.collect { |a| a.class.to_s=="String" ? "'#{a}'" : a }
  end

  # warehouse_b2b_development


  def construct_target_record(row, header)
    record = []
    get_target_table_fields_in_order(target_table).each do |field|
      record << row[header.index(rule[:map][field])]
    end
    record
  end
  def construct_source_record(row, header)
  end

end

##########################################   Migration ###########################################


class Migration
  attr_accessor  :target, :current_batch, :completed_count, :limit

  def initialize(table_name)
    # super()
    self.db = DBConfig.new()
    self.target = TargetTable.new(table_name)
    self.current_batch=0
    self.completed_count=0
    self.limit=2
    self.total_count=0
  end


  def batch_fetch_query()
    q=target.rule[:base_query][:query]+ " limit " + limit.to_s + " offset " + (completed_count).to_s
    puts q
    q
  end

  def batch_insert_query(records)
    q="insert into #{target.name} (#{target.get_fields_in_order.join(",")}) values " +
        records.collect { |o| "(#{target.string_in_quotes(o).join(",")})" }.join(",") +
        ";"
    puts q
    q
  end



  def migrate()
    # result = client.query("SELECT * FROM really_big_Table", :stream => true)
    while true
      results = db_client.query(batch_fetch_query)
      records=[]
      header = results.fields
      results.each do |row|
        records << construct_target_record(row, header)
      end



      # puts completed_count+results.size
      self.completed_count = self.completed_count+results.size.to_i
      # completed_count=f
      if results.size <= 0
        break
      else
        inserted = db_client.query(batch_insert_query(records))
      end

    end

  end

end


##########################################   AnomalyDetector ###########################################


class AnomalyDetector < BaseConfig
  attr_accessor :time_range,:target

  def initialize(target_table)
    super()
    self.target = TargetTable.new(table_name)

  end

  def batch_source_fetch_query()
    q=rule[:base_query][:query]+ " limit " + limit.to_s + " offset " + (completed_count).to_s
    puts q
    q
  end

  def batch_target_fetch_query(records)
    q="insert into #{target_table} (#{get_target_table_fields_in_order(target_table).join(",")}) values " + records.collect { |o| "(#{string_in_quotes(o).join(",")})" }.join(",") + ";"
    puts q
    q
  end

  def batch_source_validate_query(records)
    q="insert into #{target_table} (#{get_target_table_fields_in_order(target_table).join(",")}) values " + records.collect { |o| "(#{string_in_quotes(o).join(",")})" }.join(",") + ";"
    puts q
    q
  end

  def batch_target_validate_query(records)
    q="insert into #{target_table} (#{get_target_table_fields_in_order(target_table).join(",")}) values " + records.collect { |o| "(#{string_in_quotes(o).join(",")})" }.join(",") + ";"
    puts q
    q
  end

  def run_ad_for_table(table_name)

  end
end


##########################################   ActivityStatus ###########################################


class ActivityStatus
  attr_accessor :tables,:time_frame_size ,:table_states
  def initialize()

    BaseConfig::MIGRATION_RULES.keys


  end




end



Migration.new(:product_masters).migrate




#######################################################################################################


# relationships= ""
# start_result_array = ""
#can start with
#expansions - by join query
#same - table
#compression  = fiter query
#batching - id based , date based ,