Dir["../app/*.rb"].each {|file| require file }


ConfigRules::MIGRATION_RULES.each do |key,value|
  q= "CREATE TABLE #{key.to_s} ( " +value[:map].keys.collect{|c| "#{c} varchar(255)"}.join(",\n") + ")"
  puts q
  # DBConfig.new.client(q)
end


=begin

CREATE TABLE product_master (
                                 fsn varchar(255),
                                     sku varchar(255),
                                         status varchar(255),
                                                created_at timestamp,
                                                           updated_at timestamp);

CREATE TABLE inventory_defining_attributes (
                                                product_master_id varchar(255),
                                                                  seller_id varchar(255),
                                                                            status varchar(255),
                                                                                   created_at  timestamp,
                                                                                              updated_at timestamp)



end