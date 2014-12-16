class Migration
  attr_accessor  :target, :current_batch, :completed_count, :limit,:total_count,:db,:qquery

  def initialize(table_name)
    # super()
    self.db = DBConfig.new()
    self.target = TargetTable.new(table_name)
    self.current_batch=0
    self.completed_count=0
    self.limit=ConfigRules::DEFAULT_BATCH_SIZE
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

      # completed_count=f
      if results.size <= 0
        break
      else
        inserted = db.client.query(batch_insert_query(records))
        self.completed_count = self.completed_count+results.size.to_i
      end

    end

    validate_success
  end

  def validate_success
    # for post migration validation logic
    {:status=>:success,:data=>""}
  end

end
