all:
	g++ -Wall -std=c++11 -pthread -g -I. -o main MapReduceClient.h MapReduceFramework.cpp MapReduceFramework.h Barrier.cpp Barrier.h SampleClient.cpp