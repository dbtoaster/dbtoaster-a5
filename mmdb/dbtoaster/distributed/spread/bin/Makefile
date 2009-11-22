
COMPILER_DIR=../../../prototype/compiler/alpha3
COMPILER_FILES=$(wildcard $(COMPILER_DIR)/*.ml)
REDIS_DIR=../../libraries/redis-1.02
LAUNCHER_SCRIPTS=$(patsubst ../src/bin/%-launcher.rb,%.sh,$(wildcard ../src/bin/*-launcher.rb))

all: $(LAUNCHER_SCRIPTS) dbtoaster.top redis-server-recurse redis-server

$(LAUNCHER_SCRIPTS) : %.sh : ../src/bin/launcher.in
	cat ../src/bin/launcher.in | sed s/--SCRIPT--/$*-launcher.rb/ > $@ 
	chmod +x $@

dbtoaster.top: $(COMPILER_FILES)
	make -C $(COMPILER_DIR) top
	cp $(COMPILER_DIR)/dbtoaster.top ./

redis-server-recurse:
	make -C $(REDIS_DIR)

redis-server: $(REDIS_DIR)/redis-server
	cp $(REDIS_DIR)/redis-server .
	cp -r $(REDIS_DIR)/client-libraries/ruby/lib ../lib/redis

clean: 
	rm -rf $(LAUNCHER_SCRIPTS) dbtoaster.top ../lib/redis redis-server
	make -C $(COMPILER_DIR) clean
	make -C $(REDIS_DIR) clean