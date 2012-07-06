all: dbtoaster.tgz

dbtoaster.tgz: dbtoaster_dist
	tar -zcf dbtoaster.tgz dbtoaster
	cp dbtoaster.tgz \
	   dbtoaster$$(svn info | grep Revision | sed 's/Revision: /_/').tgz

dbtoaster_dist: 
	rm -rf dbtoaster
	ruby build_dist.rb

test: dbtoaster_dist
	ruby test_dist.rb

clean:
	rm -rf dbtoaster dbtoaster.tgz dbtoaster_*.tgz

.PHONY: all dbtoaster_dist test 