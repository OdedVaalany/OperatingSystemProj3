
CC=g++
CXX=g++
RANLIB=ranlib

LIBSRC=MapReduceClient.h MapReduceFramework.cpp MapReduceFramework.h Barrier.cpp Barrier.h
LIBOBJ=$(LIBSRC:.cpp=.o)

INCS=-I.
CFLAGS = -Wall -std=c++11 -pthread -g $(INCS)
CXXFLAGS = -Wall -std=c++11 -pthread -g $(INCS)

OSMLIB = libMapReduceFramework.a
TARGETS = $(OSMLIB)

TAR=tar
TARFLAGS=-cvf
TARNAME=ex3.tar
TARSRCS=$(LIBSRC) Makefile README

all: $(TARGETS)

$(TARGETS): $(LIBOBJ)
	$(AR) $(ARFLAGS) $@ $^
	$(RANLIB) $@

clean:
	$(RM) $(TARGETS) $(OSMLIB) $(OBJ) $(LIBOBJ) *~ *core

depend:
	makedepend -- $(CFLAGS) -- $(SRC) $(LIBSRC)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)

test:
	make all
	g++ -Wall -std=c++11 -pthread -g -I./ -o main SampleClient/SampleClient.cpp libMapReduceFramework.a
	echo "########\t running sample client\t########"
	./main
	g++ -Wall -std=c++11 -pthread -g -I./ -o main tests/test1.cpp libMapReduceFramework.a
	echo "########\t running test 1\t########"
	./main
	g++ -Wall -std=c++11 -pthread -g -I./ -o main tests/test2.cpp libMapReduceFramework.a
	echo "########\t running test 2\t########"
	./main
	g++ -Wall -std=c++11 -pthread -g -I./ -o main tests/test3.cpp libMapReduceFramework.a
	echo "########\t running test 3\t########"
	./main
	g++ -Wall -std=c++11 -pthread -g -I./ -o main tests/test4.cpp libMapReduceFramework.a
	echo "########\t running test 4\t########"
	./main

make check:
	make all
	g++ -Wall -std=c++11 -pthread -g -I./ -o main tests/test4.cpp libMapReduceFramework.a
	./main "/cs/usr/oded_vaalany/Documents/OS/OperatingSystemProj3/tests/TextFiles/text_file_1.txt"
	./main "/cs/usr/oded_vaalany/Documents/OS/OperatingSystemProj3/tests/TextFiles/text_file_2.txt"
	./main "/cs/usr/oded_vaalany/Documents/OS/OperatingSystemProj3/tests/TextFiles/text_file_3.txt"
	./main "/cs/usr/oded_vaalany/Documents/OS/OperatingSystemProj3/tests/TextFiles/text_file_4.txt"

make push:
	git add -A
	git commit -m "hello"
	git push
