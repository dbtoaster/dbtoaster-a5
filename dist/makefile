
dbtoaster_dist: 
	rm -rf dbtoaster
	ruby build_dist.rb
	tar -zcf dbtoaster.tgz dbtoaster

clean:
	rm -rf dbtoaster dbtoaster.tgz

.PHONY: all dbtoaster_dist