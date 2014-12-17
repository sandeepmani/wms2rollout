class TaskManager

  attr_accessor  :table_status ,:process_type,:task_list,:task_queue,:args,:table_state_fields,:activity_log
  def initialize(process_type,*args)
    self.args = args
    self.process_type = process_type
    # self.process_types = []
    # self.table_status_fields = [:state,:sucsess_time_stamp]
    self.table_status = self.read_file(table_status)
    self.activity_log = ""

    self.task_queue = []
    self.task_list = []

    (ConfigRules::MIGRATION_RULES.keys - table_status.keys).each do |table|
      self.table_status[table] = ["not migrated", nil]
    end
    write_file(table_status)

  end


  # run  #####################################################
  def run
    build_task_queue
    trigger_event(:start,:process)
    run_tasks
    trigger_event(:end,:process)
  end

  def build_task_queue()
    self.send(self.process_type,*(self.args)) # build_task_queue
  end
  #run tasks (task queues))
  def run_tasks()
    self.task_queue.each do |task|

      self.task_list << task
      trigger_event(:start,:task)

      result = self.send(task[:name],*(task[:args]))
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
      self.task_queue << {:name=>:migrate_table,:args=>[table_name]}
    end
  end


  def remigrate(table_arr)
    table_arr.each do |table_name|
      self.task_queue << {:name=>:truncate_table,:args=>[table_name]}
      self.task_queue << {:name=>:migrate_table,:args=>[table_name]}
    end
  end

  def truncate_tables(table_arr)
    table_arr.each do |table_name|
      self.task_queue << {:name=>:truncate_table,:args=>[table_name]}
    end

  end

  def run_anomaly_detector_for(table_name,start_time=nil,duration=nil)
    start_time = get_start_time(table_name) if start_time.nil?
    duration = ConfigRules::DEFAULT_DURATION if duration.nil?
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
    if ad.get_source_count < ConfigRules::DEFAULT_BATCH_SIZE
      self.task_queue << {:name=>:run_ad_for_table,:args=>[table_name,[start_time,start_time+duration]]}
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
    #todo

  end

  def add_error(table,anomaly)
    #todo

  end
  # event_listener
  def trigger_event(event_type,process_type)
    #todo

  end
  ####################################################








  # for file interactions #################################
  def read_file(file)
    {}
    #todo
  end

  def write_file(file)
    #todo

  end

  def update_file(file)
    #todo

  end
  #########################################################


end