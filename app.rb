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
  DEFAULT_BATCH_SIZE=2
  DEFAULT_DURATION = 3600 # in seconds


  MIGRATION_RULES = {

      :product_master => {
          :query_params => {
              :from_and_join => "from product_details",
              :conditions  => "",
              :additional =>"group by seller_id",

              :tables => ["product_details"], #


          },
          :map => {
              :id => :auto_increament,
              :fsn => "fsn",
              :sku => "sku",
              :status => ["binding_attribute", lambda {|text| text.to_i },lambda {|str| str.to_s }],
              :created_at => :current_time,
              :updated_at => :current_time,

          },
          :options => {
              :direct_insert => false,
              :batch_type => "timestamp",

          },

      },



      :inventory_defining_attributes => {
          :query_params => {
              :from_and_join => "from product_master  inner join product_details on product_master.fsn = product_details.fsn and product_master.sku = product_details.sku",
              :conditions  => "",
              :additional =>"",

              :tables => ["product_master","product_details"],


          },
          :map => {
              :id => :auto_increament,
              :product_master_id => "product_master.id",
              :seller_id => "product_details.seller_id",
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
  attr_accessor :db, :name, :rule, :filtered_map, :filtered_target_fields, :filtered_source_fields, :query_params
  def initialize(name)
    self.db = DBConfig.new()
    self.name=name
    self.rule = BaseConfig::MIGRATION_RULES[name]
    self.filtered_map = rule[:map].select { |key,value| value.class.name != "Symbol" }
    self.filtered_target_fields = filtered_map.keys
    self.filtered_source_fields = filtered_target_fields.collect{|f| filtered_map[f].class.name == "String" ? filtered_map[f] : filtered_map[f][0]}
    self.query_params = BaseConfig::MIGRATION_RULES[name][:query_params]
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
      source_field = filtered_map[field].class.name == "Array" ?  filtered_map[field][0] : filtered_map[field]
      value = row[header.index(source_field)]
      record << (filtered_map[field].class.name == "Array" ?  filtered_map[field][1].call(value) : value)
    end
    embed_string_in_quotes(record)
  end
  def construct_source_record(row, header)
  end

end

##########################################   Migration ###########################################


class Migration
  attr_accessor  :target, :current_batch, :completed_count, :limit,:total_count,:db,:qquery

  def initialize(table_name)
    # super()
    self.db = DBConfig.new()
    self.target = TargetTable.new(table_name)
    self.current_batch=0
    self.completed_count=0
    self.limit=BaseConfig::DEFAULT_BATCH_SIZE
    self.total_count=0
    self.insert_fields =  (target.rule[:map].keys-target.filtered_target_fields).each do |f|
      target.filtered_target_fields << f if target.rule[:map][f] != :auto_increament
    end
  end


  def batch_fetch_query()
    q=  " select " + target.filtered_source_fields,join(",")+ " " +
          target.query_params[:from_and_join] + " " +
        (target.query_params[:conditions] == "" ? "" : " where ") + target.query_params[:conditions]+ " " +
        target.query_params[:additional]+ " " +
        " limit " + limit.to_s + " " +
        " offset " + (completed_count).to_s
        puts q
    q
  end

  def batch_insert_query(records)
    q="insert into #{target.name} (#{insert_fields.join(",")}) values " +
        records.collect { |o| "(#{o.join(",")})" }.join(",") +
        ";"
    puts q
    q
  end

  def construct_insert_record(row, header)
    record = target.construct_target_record(row, header)

    (target.rule[:map].keys-target.filtered_target_fields).each do |f|
      record << "Now()" if target.rule[:map][f] != :auto_increament
    end

    record
  end


  def migrate()
    # result = client.query("SELECT * FROM really_big_Table", :stream => true)
    while true
      results = db.client.query(batch_fetch_query)
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
        inserted = db.client.query(batch_insert_query(records))
      end

    end

    validate_success
  end

  def validate_success
    # for post migration validation logic
    {:status=>:success,:data=>""}
  end

end


##########################################   AnomalyDetector ###########################################


class AnomalyDetector < BaseConfig
  attr_accessor :time_range,:target, :db ,:time_range_sql,:anomaly

  def initialize(target_table,time_range)
    # super()
    self.db = DBConfig.new()
    self.target = TargetTable.new(table_name)
    self.time_range = time_range
    self.time_range_sql = time_range.collect{|t| "'#{t.to_s}'"}
    self.anomaly = ""

  end

  def between_time_frame_condition(table)
    ["created_at","updated_at"]
        .collect{|f| " (#{table}.#{f} > #{time_range_sql[0]} and #{table}.#{f} < #{time_range_sql[1]}) "  }
        .join(" or ")
  end

  def source_query_partial()
    target.query_params[:from_and_join]+ " " +
        (target.query_params[:conditions] == "" ? "" : " where ") + target.query_params[:conditions]+ " " +
        " and " + target.query_params[:tables].collect{|c| " (#{between_time_frame_condition(c)}) " }.join(" and ") + " " +
        target.query_params[:additional]
  end

  def get_source_count()
    q= " select count(*) " + source_query_partial
    puts q
    q


  end
  def get_target_count()
    q="select count(*) from #{target.name} where #{between_time_frame_condition(target.name)}"
    puts q
    q
  end




  def batch_source_fetch_query()
    q=  " select " + target.filtered_source_fields,join(",")+ " " + source_query_partial
    puts q
    q
  end

  def batch_target_fetch_query(records)
    q="select #{filtered_target_fields.join(" , ")} from #{target.name} where #{between_time_frame_condition(target.name)}"
    puts q
    q
  end

  def batch_source_validate_query(row )
    "select count(*) from #{table} where "
    puts q
    q
  end

  def batch_target_validate_query(records)
    "select count(*) from #{table} where "
    puts q
    q
  end


  def run_ad()
    # result = client.query("SELECT * FROM really_big_Table", :stream => true)
    while true
      results = db.client.query(batch_source_fetch_query)
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
        inserted = db.client.query(batch_insert_query(records))
      end

    end

    return  validate_success
  end

  def validate_success
    # for post ad validation logic
    {:status=>:success,:data=>""}
  end
end


##########################################   TaskManager ###########################################


class TaskManager

  attr_accessor :tables ,:table_states, :task_completed ,:process_type,:task_list,:task_queue
  def initialize(task_type,args)
    self.task_type = task_type
    self.args = args
    self.table_state_fields = [:state,:time]
    self.table_states = read_file(table_status)
    self.activity_log = ""
    self.success_task = []
    self.failed_task = []
    (BaseConfig::MIGRATION_RULES.keys - table_states.keys).each do |table|
      self.table_states[table] = ["not migrated", nil]
    end
    write_file(table_status)
    self.process_types = []
  end


  # run  #####################################################
  def execute
    build_task_queue
    trigger_event(:start,:process)
    run_tasks
    trigger_event(:end,:process)
  end

  def build_task_queue()
    self.send(self.task_type,*(self.args)) # build_task_queue
  end
  #run tasks (task queues))
  def run_tasks()
    self.task_queue.each do |task|

      self.task_list << task
      trigger_event(:start,:task)

      result = self.send()
      if result[:status] == :failed
        break
      end

      self.task_list.last[:result]=result
      trigger_event(:end,:task)
    end
  end
  #######################################################################



  # Expossed Process handlers (task queue builders)################################################
  def migrate(table_arr)
    table_arr.each do |table_name|
      self.task_queue << {:type=>:migrate_table,:args=>[table_name]}
    end
  end


  def remigrate(table_arr)
    table_arr.each do |table_name|
      self.task_queue << {:type=>:truncate_table,:args=>[table_name]}
      self.task_queue << {:type=>:migrate_table,:args=>[table_name]}
    end
  end

  def truncate_tables(table_arr)
    table_arr.each do |table_name|
      self.task_queue << {:type=>:truncate_table,:args=>[table_name]}
    end

  end

  def run_anomaly_detector_for(table_name,start_time=nil,duration=nil)
    start_time = get_start_time(table_name) if start_time.nil?
    duration = BaseConfig::DEFAULT_DURATION if duration.nil?
    build_ad_queue_recursively(table_name,start_time,duration)
  end

  def resume_anomaly_detector()

  end
  ###################################################################################






  ######### tasks handlers ######################################################
  def migrate_table(table)
    Migration.new(table).migrate
  end

  def truncate_table(table)
    "truuncate table #{table}"
  end

  def run_ad_for_table(target_table,time_range)
    AnomalyDetector.new(table_name,[start_time,start_time+duration]).run_ad
  end
  ##########################################################################





  # task build helpers ##################################################
  def build_ad_queue_recursively(table_name,start_time,duration)
    ad = AnomalyDetector.new(table_name,[start_time,start_time+duration])
    if ad.get_source_count < BaseConfig::DEFAULT_BATCH_SIZE
      self.task_queue << {:type=>:run_ad_for_table,:args=>[table_name,[start_time,start_time+duration]]}
    else
      duration_split_1 = (duration/2.0 == (duration/2).to_f) ? duration/2 : duration/2 + 1
      duration_split_2 = duration/2
      build_ad_queue_recursively(table_name,start_time,duration_split_1)
      build_ad_queue_recursively(table_name,start_time+(duration_split_1) + 1,duration_split_2)
    end
  end

  def get_start_time(table_name)

  end
  #########################################################################







  # for event helper ###################################
  def update_table_state(table,change)
    self.table_states[table] = ["migrated" , Time.now]
    write_file(table_status)
  end

  def add_activity(start,type,task,params,result=nil)

  end

  def add_error(table,anomaly)

  end
    # event_listener
  def trigger_event(event_type,process_type)

  end
  ####################################################








  # for file interactions #################################
  def self.read_file(file)

  end

  def self.write_file(file)

  end

  def update_file(file)

  end
  #########################################################


end


##########################################  Usage ###########################################

Migration.new(:product_master).migrate

# TaskManager.new.(:migrate,[[:product_master,:inventory_defining_attributes]]).run
TaskManager.new.(:migrate,[:product_master,:inventory_defining_attributes]).run
TaskManager.new.(:migrate,[:product_master,:inventory_defining_attributes]).run
#
# TaskManager.new.remigrate([:product_master])
#
# TaskManager.new.run_anomaly_detector_for([:product_master])
#
# TaskManager.new.resume_anomaly_detector #






##############################################################################################


