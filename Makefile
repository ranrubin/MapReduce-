CC= g++
CPPFLAGS= -Wextra -Wall -g -std=c++11 -pthread
HEADERS= MapReduceClient.h MapReduceFramework.h
TAR_FILES= MapReduceFramework.cpp Search.cpp

all: Search libMapReduceFramework.a

MapReduceFramework.a: libMapReduceFramework.a

libMapReduceFramework.a: MapReduceFramework.o
	ar rcs MapReduceFramework.a $^

Search: Search.o MapReduceFramework.o
	$(CC) $(CPPFLAGS) $^ -o $@

%.o: %.cpp $(HEADERS)
	$(CC) $(CPPFLAGS) -c $<

tar:
	tar -cvf ex3.tar $(TAR_FILES) README Makefile

clean:
	rm -rf *.o Search libMapReduceFramework.a

.PHONY: clean all tar
