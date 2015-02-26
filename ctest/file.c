#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>

const char *msg = "2222Hi I am a very cool file! 1\n 2 Hi I am a very cool file! 2\n 3 Hi I am a very cool file! 3\n 4 Hi I am a very cool file! 4\n 5 Hi I am a very cool file! 5\n 1 Hi I am a very cool file! 1\n 2 Hi I am a very cool file! 2\n 3 Hi I am a very cool file! 3\n 4 Hi I am a very cool file! 4\n 5 Hi I am a very cool file! 5\n 1 Hi I am a very cool file! 1\n 2 Hi I am a very cool file! 2\n 3 Hi I am a very cool file! 3\n 4 Hi I am a very cool file! 4\n 5 Hi I am a very cool file! 5\n 1 Hi I am a very cool file! 1\n 2 Hi I am a very cool file! 2\n 3 Hi I am a very cool file! 3\n 4 Hi I am a very cool file! 4\n 5 Hi I am a very cool file! 5\n 1 Hi I am a very cool file! 1\n 2 Hi I am a very cool file! 2\n 3 Hi I am a very cool file! 3\n 4 Hi I am a very cool file! 4\n 5 Hi I am a very cool file! 5\n 1 Hi I am a very cool file! 1\n 2 Hi I am a very cool file! 2\n 3 Hi I am a very cool file! 3\n 4 Hi I am a very cool file! 4\n 5 Hi I am a very cool file! 5\n 1 Hi I am a very cool file! 1\n 2 Hi I am a very cool file! 2\n3 Hi I am a very cool file! 3\n 4 Hi I am a very cool file! 4\n 5 Hi";


int main()
{
    int filedesc = open("file.txt", O_CREAT | O_RDWR, 244);

    char buf[200];
    printf("test read\n");
    filedesc = 100000;
    read(100000, buf, 200);
    printf("test write\n");
    write(filedesc+1000, msg, strlen(msg));
    printf("test close\n");
    close(filedesc);
    printf("test lseek\n");
    lseek(-1, 2, 9);
    printf("test getdirentries\n");
    int basep;
    getdirentries(filedesc + 1000, buf, 20, &basep);
    printf("test open\n");
    int fd2 = open("222.txt", 222, 333);
    printf("test stat\n");
    //char buf2[2000];
    struct stat fileStat;
    stat(1, "file2.txt", &fileStat);
    printf("test unlink\n");
    unlink("filllll.txt");
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
