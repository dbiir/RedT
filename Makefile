CC=/usr/bin/g++

#CFLAGS=-Wall -Werror -std=c++11 -g3 -ggdb -O0 -fno-strict-aliasing -fno-omit-frame-pointer -D_GLIBCXX_USE_CXX11_ABI=0

CFLAGS=-Wall -std=c++1y -g3 -ggdb -O0 -fno-strict-aliasing -fno-omit-frame-pointer -I/usr/local/include -L/usr/local/lib -D_GLIBCXX_USE_CXX11_ABI=0 #-fsanitize=address
# CFLAGS=-Wall -std=c++1y -O3 -fno-strict-aliasing -I/usr/local/include -L/usr/local/lib -D_GLIBCXX_USE_CXX11_ABI=0
# CFLAGS += -fsanitize=address -fno-stack-protector -fno-omit-frame-pointer
NNMSG=./nanomsg-0.5-beta
RALLOC = ./rlib/lib

.SUFFIXES: .o .cpp .h .cc

SRC_DIRS = ./ ./benchmarks/ ./client/ ./concurrency_control/ ./storage/ ./transport/ ./system/ ./statistics/ ./rlibv2/ ./r2/ ./r2/src/#./unit_tests/ ./rlibv2/
DEPS = -I. -I./benchmarks -I./client/ -I./concurrency_control -I./storage -I./transport -I./system -I./statistics -I./rlibv2/ -I./r2/ -I./r2/src/#-I./unit_tests -I./rlibv2/

CFLAGS += $(DEPS) -D NOGRAPHITE=1 -Wno-sizeof-pointer-memaccess -ljemalloc
LDFLAGS = -Wall -L. -L$(NNMSG) -Wl,-rpath -pthread -lrt -lnanomsg -lanl -lcurl -ldl -lpthread -libverbs -ljemalloc -L$(RALLOC) -lssmalloc -lboost_system -lboost_coroutine#-lc++experimental
#LDFLAGS = -Wall -L. -L$(NNMSG) -L$(JEMALLOC)/lib -Wl,-rpath,$(JEMALLOC)/lib -pthread -gdwarf-3 -lrt -std=c++11  -lprotobuf
LDFLAGS += $(CFLAGS)
LIBS = -lrdmacm -lmemcached


DB_MAINS = ./client/client_main.cpp ./system/sequencer_main.cpp ./unit_tests/unit_main.cpp
CL_MAINS = ./system/main.cpp ./system/sequencer_main.cpp ./unit_tests/unit_main.cpp
UNIT_MAINS = ./system/main.cpp ./client/client_main.cpp ./system/sequencer_main.cpp

CPPS_DB = $(foreach dir,$(SRC_DIRS),$(filter-out $(DB_MAINS), $(wildcard $(dir)*.cpp)))
CPPS_CL = $(foreach dir,$(SRC_DIRS),$(filter-out $(CL_MAINS), $(wildcard $(dir)*.cpp)))
CPPS_UNIT = $(foreach dir,$(SRC_DIRS),$(filter-out $(UNIT_MAINS), $(wildcard $(dir)*.cpp)))

#CPPS = $(wildcard *.cpp)
OBJS_DB = $(addprefix obj/, $(notdir $(CPPS_DB:.cpp=.o)))
OBJS_CL = $(addprefix obj/, $(notdir $(CPPS_CL:.cpp=.o)))
OBJS_UNIT = $(addprefix obj/, $(notdir $(CPPS_UNIT:.cpp=.o)))

#NOGRAPHITE=1

all: rundb runcl
#unit_test

.PHONY: deps_db
deps:$(CPPS_DB)
	$(CC) $(CFLAGS) -MM $^ > obj/deps
	sed '/^[^ ]/s/^/obj\//g' obj/deps > obj/deps.tmp
	mv obj/deps.tmp obj/deps
-include obj/deps

unit_test : $(OBJS_UNIT)
#	$(CC)   -o $@ $^ $(LDFLAGS) $(LIBS)
	$(CC) -o $@ $^ $(LDFLAGS) $(LIBS)
./obj/%.o: transport/%.cpp
#	$(CC)   -c $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $<
	$(CC) -c $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $<
./obj/%.o: unit_tests/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: benchmarks/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: storage/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: system/%.cpp
	$(CC) -c -DSTATS_ENABLE=false $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: concurrency_control/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: client/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: r2/src/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: %.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<


rundb : $(OBJS_DB)
	$(CC) -o $@ $^ $(LDFLAGS) $(LIBS)
#	$(CC) -o $@ $^ $(LDFLAGS) $(LIBS)
./obj/%.o: transport/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $<
#	$(CC) -c $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $<
#./deps/%.d: %.cpp
#	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<
./obj/%.o: benchmarks/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: storage/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: system/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: statistics/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: concurrency_control/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: client/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: r2/src/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: %.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<


runcl : $(OBJS_CL)
	$(CC) -o $@ $^ $(LDFLAGS) $(LIBS)
#	$(CC) -o $@ $^ $(LDFLAGS) $(LIBS)
./obj/%.o: transport/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $<
#	$(CC) -c $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $<
#./deps/%.d: %.cpp
#	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<
./obj/%.o: benchmarks/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: storage/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: system/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: statistics/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: concurrency_control/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: client/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: r2/src/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: %.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<

.PHONY: clean
clean:
	rm -f obj/*.o obj/.depend rundb runcl runsq unit_test
