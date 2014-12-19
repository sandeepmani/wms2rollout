class TargetTable
  attr_accessor :db, :name, :rule, :filtered_map, :filtered_target_fields, :filtered_source_fields, :query_params,:sql_keywords
  def initialize(name)
    self.db = DBConfig.new()
    self.name=name
    self.rule = ConfigRules::MIGRATION_RULES[name]
    self.filtered_map = rule[:map].select { |key,value| value.class.name != "Symbol" }
    self.filtered_target_fields = filtered_map.keys
    self.filtered_source_fields = filtered_target_fields.collect{|f| filtered_map[f].class.name == "String" ? filtered_map[f] : filtered_map[f][0]}
    self.query_params = ConfigRules::MIGRATION_RULES[name][:query_params]
    self.sql_keywords = ["NULL","NOW()"]
  end

  def get_fields_in_order
    # puts db_client.query("show fields from #{table_name}").each{|c| puts c[1]}
    db.client.query("show fields from #{name}").collect { |c| c[0].to_sym }
  end

  def embed_string_in_quotes(arr)

    arr.collect { |a| (a.class.to_s=="String" && !sql_keywords.include?(a) ? "'#{a}'" : a) }
  end

  # warehouse_b2b_development

  ### do not remove
  # def construct_target_record(row, header)
  #   record = []
  #   filtered_target_fields.each do |field|
  #     source_field = filtered_map[field].class.name == "Array" ?  filtered_map[field][0] : filtered_map[field]
  #     value = row[header.index(source_field)]
  #     record << (filtered_map[field].class.name == "Array" ?  filtered_map[field][1].call(value) : value)
  #   end
  #   embed_string_in_quotes(record)
  # end

  def construct_target_record(row)
    record=(0..filtered_target_fields.size-1).collect do |index|
      field = filtered_target_fields[index]
      value = row[index]
      value = 'NULL' if value.nil?
      (filtered_map[field].class.name == "Array" ?  filtered_map[field][1].call(value) : value)
    end
    embed_string_in_quotes(record)
  end
  def construct_source_record(row)
    record=(0..filtered_target_fields.size-1).collect do |index|
      field = filtered_target_fields[index]
      value = row[index]
      (filtered_map[field].class.name == "Array" ?  filtered_map[field][2].call(value) : value)
    end
    embed_string_in_quotes(record)
  end

end
