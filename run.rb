Dir["./app/*.rb"].each {|file| require file }

# TaskManager.new(:migrate,[:product_master,:inventory_defining_attributes]).run

# TaskManager.new(:remigrate,[:product_master,:inventory_defining_attributes]).run
TaskManager.new(:truncate,[:product_master,:inventory_defining_attributes]).run
# TaskManager.new(:run_ad,:product_master,[Time.now-60 , 60]).run
# TaskManager.new(:resume_ad).run


#############


