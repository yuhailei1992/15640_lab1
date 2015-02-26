#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#define MAXWRITE 4000
const char *msg = "2222Hi I am a very cool file! 1\n 2 Hi I am a very cool file! 2\n 3 Hi I am a very cool file! 3\n 4 Hi I am a very cool file! 4\n 5 Hi I am a very cool file! 5\n 1 Hi I am a very cool file! 1\n 2 Hi I am a very cool file! 2\n 3 Hi I am a very cool file! 3\n 4 Hi I am a very cool file! 4\n 5 Hi I am a very cool file! 5\n 1 Hi I am a very cool file! 1\n 2 Hi I am a very cool file! 2\n 3 Hi I am a very cool file! 3\n 4 Hi I am a very cool file! 4\n 5 Hi I am a very cool file! 5\n 1 Hi I am a very cool file! 1\n 2 Hi I am a very cool file! 2\n 3 Hi I am a very cool file! 3\n 4 Hi I am a very cool file! 4\n 5 Hi I am a very cool file! 5\n 1 Hi I am a very cool file! 1\n 2 Hi I am a very cool file! 2\n 3 Hi I am a very cool file! 3\n 4 Hi I am a very cool file! 4\n 5 Hi I am a very cool file! 5\n 1 Hi I am a very cool file! 1\n 2 Hi I am a very cool file! 2\n 3 Hi I am a very cool file! 3\n 4 Hi I am a very cool file! 4\n 5 Hi I am a very cool file! 5\n 1 Hi I am a very cool file! 1\n 2 Hi I am a very cool file! 2\n3 Hi I am a very cool file! 3\n 4 Hi I am a very cool file! 4\n 5 Hi";


int main()
{
    int filedesc = open("file.txt", O_CREAT | O_RDWR, 244);
    char *buf = (char *)malloc(MAXWRITE);
    int i = 0;
    while (i < MAXWRITE)
    {
        buf[i] = 'a' + i % 26;
        i++;
    }
    if(filedesc < 0)
    {
    	printf("filedesc is less than 0\n");
    	return 1;
    }

    /*if(write(filedesc, msg, strlen(msg)) < 0)
    {
        write(2,"There was an error writing to testfile.txt\n",43);
        return 1;
    }*/
    write(filedesc, buf, strlen(buf));
    close(filedesc);

    // test getdirentries

    /*
    int fd = open("testfile.txt", O_WRONLY | O_CREAT, 244);
    char *buf = (char *)malloc(1051);
    size_t readcount = read(fd, (void *)buf, 1050);
    buf[1050] = 0;
    printf("The content is %s\n", buf);
    printf("checksum:: head is %c, tail is %c\n", buf[0], buf[1049]);
    free(buf);
    */
    return 0;
}
