
FILES=gen.o driver.o bm_utils.o load_stub.o print.o build.o rng64.o permute.o rnd.o text.o speed_seed.o

LFLAGS=
FLAGS=-DLINUX -D_POSIX_SOURCE -Wall -Werror
FLAGS += -DDISABLE_PINGS

all: streamgen

streamgen: $(FILES) $(THRIFT_OUT)
	gcc $(FILES) $(LFLAGS) $(FLAGS) -o $@

$(FILES) : %.o : %.c
	gcc $(patsubst %.o,%.c,$@) $(LFLAGS) $(FLAGS) -c

clean:
	rm -rf streamgen *.o