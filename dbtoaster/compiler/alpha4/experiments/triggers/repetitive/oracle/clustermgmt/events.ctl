load data
 infile '@@PATH@@sl_servers.dat'
 into table agenda
 fields terminated by "," ( event, rackid, load )