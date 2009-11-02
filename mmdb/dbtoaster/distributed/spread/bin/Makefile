
COMPILER_DIR=../../../prototype/compiler/alpha3
COMPILER_FILES=$(wildcard $(COMPILER_DIR)/*.ml)
LAUNCHER_SCRIPTS=$(patsubst ../src/bin/%-launcher.rb,%.sh,$(wildcard ../src/bin/*-launcher.rb))

all: $(LAUNCHER_SCRIPTS) dbtoaster.top

$(LAUNCHER_SCRIPTS) : %.sh : ../src/bin/launcher.in
	cat ../src/bin/launcher.in | sed s/--SCRIPT--/$*-launcher.rb/ > $@ 
	chmod +x $@

dbtoaster.top: $(COMPILER_FILES)
	make -C $(COMPILER_DIR) top
	cp $(COMPILER_DIR)/dbtoaster.top ./

clean: 
	rm -f $(LAUNCHER_SCRIPTS) dbtoaster.top
	make -C $(COMPILER_DIR) clean