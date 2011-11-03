load data
 infile '@@PATH@@Events.dbtdat'
 into table agenda
 fields terminated by ","      
 ( schema, event, t, id, broker_id, volume, price )