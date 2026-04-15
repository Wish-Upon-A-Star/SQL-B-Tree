CC ?= gcc
CFLAGS ?= -fdiagnostics-color=always -g
TARGET ?= sqlsprocessor
SRC = main.c
SQL ?= demo_bptree.sql

.PHONY: all build run demo-bptree clean

all: build

build: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET)

run: $(TARGET)
	./$(TARGET) $(SQL)

demo-bptree: $(TARGET)
	./$(TARGET) demo_bptree.sql

clean:
	rm -f $(TARGET)
