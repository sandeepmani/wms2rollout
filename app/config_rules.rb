class ConfigRules
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
