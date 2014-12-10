require "mysql2"


class WHConfig
  attr_accessor :db_client, :migrations, :db_details

  def initialize
    # batch_size=100

    self.db_details={
        :host => "localhost",
        :username => "root",
        :database => "warehouse_b2b_development",

    }
    Mysql2::Client.default_query_options.merge!(:as => :array)
    self.db_client = Mysql2::Client.new(db_details)
    self.migrations = {
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
                :status => ["status", lambda {|text| text.to_s }],
                :created_at => :current_time,
                :updated_at => :current_time,

            },

        },

    }


  end

  def get_target_table_fields_in_order(table_name)
    # puts db_client.query("show fields from #{table_name}").each{|c| puts c[1]}
    db_client.query("show fields from #{table_name}").collect { |c| c[0].to_sym }
  end

end


class Migration < WHConfig
  attr_accessor :target_table, :total_count, :errors, :current_batch, :completed_count, :limit, :rule

  def initialize(target_table)
    super()
    self.target_table=target_table
    self.current_batch=0
    self.completed_count=0
    self.limit=2
    self.total_count=0
    # self.field_index =  Hash[fields.map.with_index.to_a]
    self.rule = self.migrations[target_table]
    puts self
  end


  def batch_fetch_query()
    q=rule[:base_query][:query]+ " limit " + limit.to_s + " offset " + (completed_count).to_s
    puts q
    q
  end

  def batch_insert_query(records)
    q="insert into #{target_table} (#{get_target_table_fields_in_order(target_table).join(",")}) values " + records.collect { |o| "(#{string_in_quotes(o).join(",")})" }.join(",") + ";"
    puts q
    q
  end

  def string_in_quotes(arr)
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


class AD < WHConfig
  def initialize(target_table)

  end

  def run_ad_for_table(table_name)

  end
end


class ActivityState

end

Migration.new(:product_masters).migrate





# relationships= ""
# start_result_array = ""
#can start with
#expansions - by join query
#same - table
#compression  = fiter query
#batching - id based , date based ,