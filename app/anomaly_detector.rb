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

  def build_where_conition(fields,values)
    " " + (0..fields-1).collect do |index|
      "#{fields[index]} = #{values[index]}"
    end.join(" and ") + " "
  end




  # for source to target validation ###########################
  def source_query_partial()
        "from (" +

        " select " + target.filtered_source_fields.join(",") +
        target.query_params[:from_and_join]+ " " +
        (target.query_params[:conditions] == "" ? "" : " where ") + target.query_params[:conditions]+ " " +
        " and " + target.query_params[:tables].collect{|c| " (#{between_time_frame_condition(c)}) " }.join(" and ") + " " +
        target.query_params[:additional] +

        ")" +
        "group by (#{target.filtered_source_fields.join(',')})"
  end


  def get_source_count()
    q= " select count(*) " + source_query_partial
    puts q
    q
  end

  def batch_source_fetch_query()
    q=  " select " + target.filtered_source_fields.join(",")+ " , COUNT(*)  " + source_query_partial
    puts q
    q
    #todo
  end
  def batch_target_validate_query(records)
    "select * from " +
        "(set @num := 0 "+
        records.collect{|row| "(select #{target.filtered_target_fields.join(",")} ,  count(*) as actual_count ,@num := #{row.last} as validation_count from #{target.name} where
          #{build_where_conition(target.filtered_target_fields,target.construct_target_record(row[0..target.filtered_target_fields-1]))}) " }.join(" UNION ALL ")   +

        ") where validation_count > actual_count"
    puts q
    q

  end


  # for target to source validation #####################################
  def get_target_count()
    q="select count(*) " + "from #{target.name} where #{between_time_frame_condition(target.name)}" + "group by (#{target.filtered_target_fields.join(',')})"
    puts q
    q
  end
  def batch_target_fetch_query()
    q="select #{filtered_target_fields.join(" , ")} , count(*)" + "from #{target.name} where #{between_time_frame_condition(target.name)}" + "group by (#{target.filtered_target_fields.join(',')})"
    puts q
    q
    #todo
  end



  def batch_source_validate_query(records)
        "select * from " +
        "(set @num := 0 "+
        records.collect{|row|

          "(select #{target.filtered_source_fields.join(",")} ,  count(*) as actual_count ,@num := #{row.last} as validation_count "+
            target.query_params[:from_and_join]+ " " +
            "where"+ target.query_params[:conditions]+ " and " +
            " #{build_where_conition(target.filtered_source_fields,target.construct_source_record(row[0..target.filtered_source_fields-1]))}) " +
          target.query_params[:additional]

        }.join(" UNION ALL ")   +

        ") where validation_count > actual_count"

    puts q
    q

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

    validate_success
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
