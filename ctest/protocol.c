#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>

#define MAXMSGLEN 100

int main() {
    char *pathname = "/user/caesar";
    char msg[MAXMSGLEN];
    // configure the message
	int msg_i = 0;
	msg[msg_i++] = 'o';
	msg[msg_i++] = ' ';

    int pathname_i = 0;
    while (pathname[pathname_i] != 0) {
        msg[msg_i++] = pathname[pathname_i++];
    }
	msg[msg_i++] = ' ';
    // now msg_i points to the next available position
    // transmit  the flag to server
    int *p = (int *)(msg + msg_i);
    printf("The msg and p pointers are %p and %p \n", msg, p);
	*p = 0x65666768;
	// append the terminator
	msg_i += 4;
	msg[msg_i] = 0;
	printf("The message is %s\n", msg);

	printf("then... another test\n");

	char msg2[MAXMSGLEN];
	//memset(msg2, 0, MAXMSGLEN);

	strcat(msg2, "hello");
	strcat(msg2, " ");
	strcat(msg2, "darkness my old friend");
	char num[5];
	int *p2 = (int *)num;
	*p2 = 0x65666768;
	num[4] = 0;
	strcat(msg2, num);

	printf("The concatenated string is %s\n", msg2);
	// decode
	int len = strlen(msg2);
	printf("the length is %d\n", len);
	/*
	char fn[MAXMSGLEN];

	int i = 1;
	int fn_i = 0;
	while (i < len && msg2[i] != ' ')
	{
		fn[fn_i++] = msg2[i++];
	}
	fn[fn_i] = 0;

	printf("the filename is %s\n", fn);

	i++;
	while (i < len && msg2[i] != '') */
	int number;
	char mode[100];
	char filen[100];
	char *msg3 = "o /user/cat 56";
	sscanf(msg3, "%s %s %d", mode, filen, &number);

	printf("%s %s, %d\n", mode, filen, number);

	char *pathname4 = "readme";
	int flags = 0;
	printf("the pathname is %s\n", pathname4);
    printf("the flag is %d\n", flags);
    char msg4[MAXMSGLEN];
    // configure the message. format: o [filepath] [flag]
    strcat(msg4, "o ");
    strcat(msg4, pathname4);
    strcat(msg4, " ");
    char num4[5];
    int *p4 = (int *)num4;
    *p4 = 65;
    num4[4] = 0;
    strcat(msg4, num4);
    printf("the message is %s\n", msg4);
    char str[10];
    sprintf(str, "%d", 143);
    printf("the integer is %s\n", str);
	return 0;
}

