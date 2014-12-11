##########################################   DBConfig ###########################################

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

##########################################   BaseConfig ###########################################


class BaseConfig
  DEFAULT_BATCH_SIZE=100
  DEFAULT_DURATION = 60 # in minutes


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
    self.limit=BaseConfig::DEFAULT_BATCH_SIZE
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

    return  validate_success
  end

  def validate_success
    :sucess
  end

end


##########################################   AnomalyDetector ###########################################


class AnomalyDetector < BaseConfig
  attr_accessor :time_range,:target

  def initialize(target_table,time_range)
    # super()
    self.db = DBConfig.new()
    self.time_range = time_range
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

  def run_ad(table_name)

  end
end


##########################################   TaskManager ###########################################


class TaskManager
  attr_accessor :tables,:time_frame_size ,:table_states, :task_completed ,:task_type
  def initialize()
    self.table_states = read_file(table_status)
    (BaseConfig::MIGRATION_RULES.keys - table_states.keys).each do |table|
      self.table_states[table] = ["not migrated", nil]
    end
    write_file(table_status)
    # self.task_type = task_type


  end



  def self.read_file()

  end

  def self.write_file()

  end

  def migrate(table_arr)
    table_arr.each do |table|
     task = Migration.new(table).migrate
     if task != :success
       break
     else
       self.table_states[table] = ["migrated" , Time.now]
     end
    end
    write_file(table_status)
  end



  def remigrate(table_arr)

  end

  def run_anomaly_detector_for(table_name,)

    task = AnomalyDetector.new(table).migrate
    self.table_states[table_name] = ["migrated" , Time.now]
    write_file(table_status)
  end

  def resume_anomaly_detector

  end

end


##########################################   Usage ###########################################

TaskManager.new.migrate([:product_master,:in])

TaskManager.new.remigrate([:product_master])

TaskManager.new.run_anomaly_detector_for([:product_master])

TaskManager.new.resume_anomaly_detector #






#######################################################################################################


# relationships= ""
# start_result_array = ""
#can start with
#expansions - by join query
#same - table
#compression  = fiter query
#batching - id based , date based ,