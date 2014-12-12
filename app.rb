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
          :query_params => {
              :select => "fsn, sku",
              :from_and_join => "from product_details",
              :where  => "",
              :additional =>"group by seller_id",

              :tables => ["product_details"],


          },
          :map => {
              :id => :auto_increament,
              :fsn => "fsn",
              :sku => "sku",
              :status => ["status", lambda {|text| text.to_s },lambda {|str| str.to_s }],
              :created_at => :current_time,
              :updated_at => :current_time,

          },
          :options => {
              :direct_insert => false,
              :batch_type => "timestamp",

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
    self.rule = self.migration_rules[target_table]
    self.filtered_map = rule[:map].select { |key,value| value.class.name != "Symbol" }
    # self.reverse_filterd_map =
    self.filtered_target_fields = filtered_map.keys
    self.filtered_source_fields = target_fields.collect{|f| filtered_map[f].class.name == "String" ? filtered_map[f] : filtered_map[f][0]}

    self.query_params = self.migration_rules[target_table][:query_params]
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
    filtered_target_fields.each do |field|
      value = row[header.index(rule[:map][field])]
      record < filtered_map[field].class.name == "String" ? value : filtered_map[field][1].call(value)
    end
    record
  end
  def construct_source_record(row, header)
  end

end

##########################################   Migration ###########################################


class Migration
  attr_accessor  :target, :current_batch, :completed_count, :limit,:total_count

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
    q=  " select " + query_params[:select]+
         query_params[:from_and_join]+
        (query_params[:where] = "" ? "" : " where ") + query_params[:where]+
         query_params[:additional]+
        " limit " + limit.to_s +
        " offset " + (completed_count).to_s
    puts q
    q
  end

  def batch_insert_query(records)
    q="insert into #{target.name} (#{get_insert_fields.join(",")}) values " +
        records.collect { |o| "(#{target.string_in_quotes(o).join(",")})" }.join(",") +
        ";"
    puts q
    q
  end

  def get_insert_fields
    (target.rule[:map].keys-target.filtered_target_fields).each do |f|
      target.filtered_target_fields << f if target.rule[:map][f] != :auto_increament
    end
  end

  def construct_insert_record(row, header)
    record = construct_target_record(row, header)

    (target.rule[:map].keys-target.filtered_target_fields).each do |f|
      record << "Now()" if target.rule[:map][f] != :auto_increament
    end

    record
  end


  def migrate()
    # result = client.query("SELECT * FROM really_big_Table", :stream => true)
    while true
      results = db_client.query(batch_fetch_query)
      records=[]
      header = results.fields
      results.each do |row|
        records << construct_insert_record(row, header)
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
    :success
  end

end


##########################################   AnomalyDetector ###########################################


class AnomalyDetector < BaseConfig
  attr_accessor :time_range,:target

  def initialize(target_table,time_range)
    # super()
    self.db = DBConfig.new()
    self.target = TargetTable.new(table_name)
    
    self.time_range = time_range
    self.time_range_sql = time_range


  end



  def between_time_frame_condition(table)
    ["created_at","updated_at"]
        .collect{|f| " (#{table}.#{f} > #{time_range_sql[0]} and #{table}.#{f} < #{time_range_sql[1]}) "  }
        .join(" or ")
  end

  end

  def batch_source_fetch_query()
    q=  " select " + query_params[:select]+
        query_params[:from_and_join]+
        (query_params[:where] = "" ? "" : " where ") + query_params[:where]+
    " and " + query_params[:tables].collect{|c| " (#{between_time_frame_condition(c)}) " }.join(" and ")
        query_params[:additional]+

    puts q
    q
  end

  def batch_target_fetch_query(records)
    q="select #{filtered_target_fields.join(" , ")} from #{target.name} where #{between_time_frame_condition(target.name)}"
    puts q
    q
  end

  def batch_source_validate_query(records)
    puts q
    q
  end

  def batch_target_validate_query(records)
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


