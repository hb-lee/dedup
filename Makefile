CC = g++
PROM = dedup
SOURCE = dedup.cpp md5.cpp
$(PROM): $(SOURCE)
	$(CC) -std=c++11 -Wall -g -lm -o $(PROM) $(SOURCE)
