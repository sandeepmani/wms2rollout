class TaskManager

  attr_accessor  :table_status ,:process_name,:task_list,:task_queue,:args,:table_state_fields,:activity_log,:anomoly
  def initialize(process_name,*args)
    self.args = args
    self.process_name = process_name
    # self.process_names = []
    # self.table_status_fields = [:state,:sucsess_time_stamp]
    self.table_status = self.read_data("table_status")
    # self.activity_log = []
    # self.anomoly = []

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
    self.send(self.process_name,*(self.args)) # build_task_queue
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
    "truncate table #{table}"
    {:status=>:success,:data=>""}
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
    write_data("table_status",table_status)
  end

  def add_activity(*args)
    append_data(activity_log,args.join(" : "))
  end

  def add_error(*args)
    append_data(anomaly,args.to_json
  end
  # event_listener
  def trigger_event(event,cat)
    # [start,type,task  ,params, result=nil ,timestamp,duration]

    activity = []
    activity << "#{event.to_s} #{cat.to_s} #{}"
    activity <<  cat == :process ? self.process_name.to_s : self.task_list.last[:name].to_s
    activity << "at #{Time.now}"


    if event == :start
      activity <<  cat == :process ? self.process_name.args : self.task_list.last[:args].to_s
    else
      activity <<  cat == :process ? "sucess" : self.task_list.last[:result].to_s
      activity << "Time Taken 5 mins"
      update_table_state()
    end

    if event == :end && self.task_list.last[:result] == :success_with_anomaly
      add_error()
    end
    
    add_activity(activity)
    
    
    #process
    if ssss == :process
      if event == :start
        
      else

      end
    end

  end
  ####################################################








  # for file interactions #################################
  def read_data(file)
    JSON.parse(File.read("./data_files/#{file}.json"))
  end

  def write_data(file,content)
    File.open("./data_files/#{file}.json", 'w') do |f|
      puts content
    end
  end

  def append_data(file_name,content)
    File.open("./data_files/#{file_name}.json", 'a') do |f|
      puts content
    end
  end
  #########################################################


end
