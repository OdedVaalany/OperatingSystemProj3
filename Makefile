
CC=g++
CXX=g++
RANLIB=ranlib

LIBSRC=MapReduceClient.h MapReduceFramework.cpp MapReduceFramework.h Barrier.cpp Barrier.h
LIBOBJ=$(LIBSRC:.cpp=.o)

INCS=-I.
CFLAGS = -Wall -std=c++11 -pthread -g $(INCS)
CXXFLAGS = -Wall -std=c++11 -pthread -g $(INCS)

OSMLIB = libuthreads.a
TARGETS = $(OSMLIB)

TAR=tar
TARFLAGS=-cvf
TARNAME=ex2.tar
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
	g++ -Wall -std=c++11 -pthread -g -I. -o main MapReduceClient.h MapReduceFramework.cpp MapReduceFramework.h Barrier.cpp Barrier.h SampleClient.cpp

make push:
	git add -A
	git commit -m "hello"
	git push