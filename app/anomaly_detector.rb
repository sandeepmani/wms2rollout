class AnomalyDetector
  attr_accessor :time_range,:target, :db ,:time_range_sql,:anomaly

  def initialize(target_table,time_range)
    # super()
    self.db = DBConfig.new()
    self.target = TargetTable.new(table_name)
    self.time_range = time_range
    self.time_range_sql = time_range.collect{|t| "'#{t.to_s}'"}
    self.anomaly = {}

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
    #todo
  end

  def batch_target_fetch_query()
    q="select #{filtered_target_fields.join(" , ")} from #{target.name} where #{between_time_frame_condition(target.name)}"
    puts q
    q
    #todo
  end



  def batch_source_validate_query(records)
    "select count(*) from #{table} where "
    puts q
    q
    #todo
  end

  def batch_target_validate_query(records)
    "select count(*) from #{table} where "
    puts q
    q
    #todo
  end


  def run_ad()
    #for source to target
    records = db.client.query(batch_source_fetch_query)
    result = db.client.query(batch_target_validate_query(records))
    if results.size > 0
      anomaly[:source] = results.collect{|r| r}
    end
    #for target to source
    records = db.client.query(batch_target_fetch_query)
    result = db.client.query(batch_source_validate_query(records))
    if results.size > 0
      anomaly[:target] = results.collect{|r| r}
    end

    return  validate_success
  end

  def validate_success
    # for post ad validation logic
    if anomaly.empty?
      {:status=>:success,:data=>""}
    else
      {:status=>:success_with_anomolies,:data=>anomaly}
    end


  end
end
