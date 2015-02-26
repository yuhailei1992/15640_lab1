PROGS=server

all: mylib.so $(PROGS)

server: server.c
	gcc server.c -L../lib -I../include -ldirtree -o server
mylib.o: mylib.c
	gcc -Wall -fPIC -DPIC -I../include -L../lib -c mylib.c

mylib.so: mylib.o
	ld -shared -o mylib.so mylib.o -ldl

clean:
	rm -f *.o *.so server
