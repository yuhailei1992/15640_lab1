/******************************************************************************
 * author: haileiy@andrew.cmu.edu
 * date:   2015 / 01 / 28
 *****************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <string.h>
#include <unistd.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <dlfcn.h>
#include <sys/stat.h>
#include <dirent.h>
#include "dirtree.h"

#define MAXMSGLEN 1024 // max len of messages
#define INTSTRLEN 16 // length of short messages (return value + errno)

/******************************************************************************
 * dfs will depth-first-traverse the tree with root root, and store the
 * serialized tree in buf
 *****************************************************************************/
void dfs(struct dirtreenode *root, char **buf) {
    //fprintf(stderr,"name %s, num_child %d\n", root->name, root->num_subdirs);
    int len = strlen(root->name);
    *(int *)*buf = len;
    *buf += sizeof(int);
    memcpy(*buf, root->name, len);
    *buf += len;
    *(int *)*buf = root->num_subdirs;
    *buf += sizeof(int);
    // recursively process all the childs
    int i = 0;
    while (i < root->num_subdirs) {
        dfs(root->subdirs[i], buf);
        i++;
    }
}

/******************************************************************************
 * processOpen will unmarshall the msg from client, perform the open syscall,
 * marshall the retval and errno into retmsg, which will be sent back to client
 *
 * msg format: [messagetype][flags][m][filepath]
 * retmsg format: [return value][errno]
 *****************************************************************************/
void processOpen (char *msg, char *retmsg) {

    // unmarshall parameters from msg
    int idx = 1; //jump over messagetype
    int flags = *(int *)(msg + idx);
    idx += sizeof(int);
    mode_t m = *(mode_t *)(msg + idx);
    idx += sizeof(mode_t);
    char filepath[MAXMSGLEN];
    strcpy(filepath, msg + idx);
    fprintf(stderr,"server:: open. flags = %d\n", flags);
    // marshall retmsg. format: [retval][errno]
    int rv = open(filepath, flags, m);
    *(int *)(retmsg) = rv;
    *(int *)(retmsg + sizeof(int)) = errno;
    fprintf(stderr,"server:: open. errno = %d\n", errno);
}

/******************************************************************************
 * processClose will unmarshall the msg from client, perform the close syscall,
 * marshall the retval and errno into retmsg, which will be sent back to client
 *
 * msg format: [messagetype][fd]
 * retmsg format: [return value][errno]
 *****************************************************************************/

void processClose (char *msg, char *retmsg) {

    // unmarshall parameters from msg. format: o[fd]
    int idx = 1;
    int fd = *(int *)(msg + idx);
    fprintf(stderr,"server:: close. fd = %d\n", fd);
    // marshall parameters in retmsg
    int rv = close(fd);
    *(int *)retmsg = rv;
    *(int *)(retmsg + sizeof(int)) = errno;

    fprintf(stderr,"server:: close. errno = %d\n", errno);
}


/******************************************************************************
 * processWrite will unmarshall the msg from client, perform the write syscall,
 * marshall the retval and errno into retmsg, which will be sent back to client
 *
 * write is special. It requires four tcp transmissions, rather than two.
 * 1:  client send fd and count to server. The server dynamically allocates a
 *     buffer to store the 3rd message
 * 2:  server sends a short ack back to client
 * 3:  upon receiving ack, the client sends the content to write to server
 * 4:  server receives content, write it to file, then marshall the retval and
 *     errno to retmsg, which will be sent back to client
 *
 * msg1 format: [messagetype][fd][count]
 * msg2 format: [y]
 * msg3 format: [content]
 * msg4 format: [return value][errno]
 *****************************************************************************/

void processWrite(char *msg, char *retmsg, int sessfd) {
    // unmarshall the 1st msg
    int idx = 1;
    size_t count = *(size_t *)(msg + idx);
    idx += sizeof(size_t);
    int fd = *(int *)(msg + idx);
    char *writebuf = (char *)malloc(count);
    // send ack(2nd msg) to client
    char ack[INTSTRLEN];
    memset(ack, 0, INTSTRLEN);
    ack[0] = 'y';

    char tmpbuf[MAXMSGLEN];
    memset(tmpbuf, 0, MAXMSGLEN);

    send (sessfd, ack, INTSTRLEN, 0);
    // receive content(3rd msg) via tcp
    int sum_cnt = 0;
    while (sum_cnt < count)
    {
        int part_cnt = recv(sessfd, tmpbuf, MAXMSGLEN, 0);
        memcpy(writebuf+sum_cnt, tmpbuf, part_cnt);
        sum_cnt += part_cnt;
    }

    fprintf(stderr,"server:: write, fd = %d, count = %lu\n", fd, count);
    // marshall retval and errno, send back to client (4th msg)
    ssize_t rv = write(fd, (const void *)writebuf, count);
    *(ssize_t *)(retmsg) = rv;
    *(int *)(retmsg + sizeof(ssize_t)) = errno;

    fprintf(stderr,"server:: write. errno = %d\n", errno);
    free(writebuf);
}

/******************************************************************************
 * processRead will unmarshall the msg from client, perform the read syscall,
 * marshall the retval, errno and content into retmsg, then send the retmsg
 * back to client
 *
 * msg format: [messagetype][fd][count]
 * retmsg format: [return value][errno][content]
 *****************************************************************************/

void processRead(char *msg, char *retmsg, int sessfd) {
    // unmarshall msg.
    int idx = 1;
    int fd = *(int *)(msg + idx);
    idx += sizeof(int);
    size_t count = *(size_t *)(msg + idx);
    fprintf(stderr,"server:: read fd is %d, read count is %lu\n", fd, count);

    size_t retmsglen = count + sizeof(int) + sizeof(ssize_t);
    char *tmp = (char *)malloc(count);
    char *readbuf = (char *)malloc(retmsglen);
    ssize_t rv = read(fd, (void *)tmp, count);

    // marshall retval, errno and read content
    *(ssize_t *)readbuf = rv;
    *(int *)(readbuf + sizeof(ssize_t)) = errno;
    if (rv >= 0)
    {
        memcpy(readbuf + sizeof(ssize_t) + sizeof(int), tmp, rv);
        send(sessfd, readbuf, retmsglen, 0);
    } else {
        memset(readbuf + sizeof(ssize_t) + sizeof(int), 0, count);
        send(sessfd, readbuf, retmsglen, 0);
    }
    free(readbuf);
    free(tmp);
    fprintf(stderr,"server:: read. errno = %d\n", errno);
}

/******************************************************************************
 * processLseek will unmarshall the msg from client, perform the lseek syscall,
 * marshall the retval, errno and content into retmsg, then send the retmsg
 * back to client
 *
  * msg format: [messagetype][fd][count][whence]
 * retmsg format: [return value][errno]
 *****************************************************************************/

void processLseek(char *msg, char *retmsg) {
    int idx = 1;// jump over messagetype
    int fd = *(int *)(msg + idx);
    idx += sizeof(int);
    off_t offset = *(off_t *)(msg + idx);
    idx += sizeof(off_t);
    int whence = *(int *)(msg + idx);
    // config retmsg
    int retidx = 0;
    off_t rv= lseek(fd, offset, whence);
    *(off_t *)(retmsg + retidx) = rv;
    retidx += sizeof(off_t);
    *(int *)(retmsg + retidx) = errno;
    fprintf(stderr,"server:: lseek. errno = %d\n", errno);
}

/******************************************************************************
 * processStat will unmarshall the msg from client, perform the stat syscall,
 * marshall the retval, errno and buf into retmsg, which will be sent back to
 * client
 *
 * msg format: [messagetype][version][path strlen][path]
 * retmsg format: [retval][errno][buf]
 *****************************************************************************/

void processStat(char *msg, char *retmsg) {
    // unmarshall
    int idx = 1;
    int version = *(int *)(msg + idx);
    idx += sizeof(int);
    int pathlen = *(int *)(msg + idx);
    idx += sizeof(int);
    fprintf(stderr,"server:: stat. version = %d, pathlen = %d\n", version, pathlen);

    // marshall retmsg
    *(int *)retmsg = __xstat(version, (const char *)(msg + idx), (struct stat *)(retmsg + sizeof(int) + sizeof(int)));
    *(int *)(retmsg + sizeof(int)) = errno;

    fprintf(stderr,"server:: stat. errno = %d\n", errno);
}

/******************************************************************************
 * processUnlink will unmarshall the msg from client, perform unlink syscall,
 * marshall the retval, errno and buf into retmsg, which will be sent back to
 * client
 *
 * msg format: [messagetype][pathname length][pathname]
 * retmsg format: [retval][errno]
 *****************************************************************************/

void processUnlink(char *msg, char *retmsg) {
    int idx = 1;
    int count = *(int *)(msg + idx);
    idx += sizeof(int);
    *(int *) retmsg = unlink((const char *)(msg + idx));
    *(int *)(retmsg + sizeof(int)) = errno;
    fprintf(stderr,"server:: unlink. errno = %d\n", errno);
}

/******************************************************************************
 * processGetdirentries unmarshalls the msg from client, perform getdirentries
 * syscall, marshall the retval, errno and buf into retmsg, which will be sent
 * back to client
 *
 * msg format: [messagetype][fd][nbytes]
 * retmsg format: [retval][errno][buf][basep]
 *****************************************************************************/

void processGetdirentries(char *msg, int sessfd) {

    // unmarshall.
    int idx = 1;
    int fd = *(int *)(msg + idx);
    idx += sizeof(int);
    size_t nbytes = *(size_t *)(msg + idx);
    idx += sizeof(size_t);

    size_t retmsglen = nbytes + sizeof(ssize_t) + sizeof(int) + sizeof(off_t);
    char *retmsg = (char *)malloc(retmsglen);
    off_t offset = 0;
    *(ssize_t *)retmsg = getdirentries(fd, retmsg + sizeof(ssize_t) + sizeof(off_t) + sizeof(int), nbytes, &offset);
    // prepare ret msg
    *(int *)(retmsg + sizeof(ssize_t)) = errno;
    *(off_t *)(retmsg + sizeof(ssize_t) + sizeof(off_t)) = offset;
    send(sessfd, retmsg, retmsglen, 0);
    free(retmsg);
    fprintf(stderr,"server:: getdirentries. errno = %d\n", errno);
}

/******************************************************************************
 * processGetdirtree unmarshall the msg from client, call getdirtree syscall,
 * marshall the retval, errno and buf into retmsg, which will be sent back to
 * client
 *
 * The dirtree is serialized so that it can be sent via tcp
 *
 * msg format: [messagetype][path length][path]
 * retmsg format: [retval][errno][buf]
 *****************************************************************************/

void processGetdirtree(char *msg, int sessfd) {
    int idx = 0;
    idx++;
    int len = *(int *)(msg + idx);
    idx += sizeof(int);
    char path[MAXMSGLEN];
    memset(path, 0, MAXMSGLEN);
    memcpy(path, msg + idx, len);

    struct dirtreenode *root = getdirtree(path);

    char *retmsg = (char *)malloc(MAXMSGLEN);

    int retidx = 0;
    *(int *)retmsg = errno;
    retidx += sizeof(int);
    char *p = retmsg + retidx;
    dfs(root, &p);
    send(sessfd, retmsg, MAXMSGLEN, 0);
    freedirtree(root);
    free(retmsg);
    fprintf(stderr,"server:: getdirtree. errno = %d\n", errno);
}

/******************************************************************************
 * The server opens a serverport, listens to that port. When a client connects
 * this port, the server will fork a child process to handle that client, till
 * the client finishes the program.
 *****************************************************************************/

int main(int argc, char**argv) {
    char buf[MAXMSGLEN+1];
    memset(buf, 0, MAXMSGLEN+1);

    char *serverport;
    unsigned short port;
    int sockfd, sessfd, rv;
    struct sockaddr_in srv, cli;
    socklen_t sa_size;

    // Get environment variable indicating the port of the server
    serverport = getenv("serverport15440");
    if (serverport) port = (unsigned short)atoi(serverport);
    else port=15440;

    // Create socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) err(1, 0);

    // setup address structure to indicate server port
    memset(&srv, 0, sizeof(srv));
    srv.sin_family = AF_INET;
    srv.sin_addr.s_addr = htonl(INADDR_ANY);	// don't care IP address
    srv.sin_port = htons(port);			// server port

    // bind to our port
    rv = bind(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
    if (rv < 0) err(1, 0);

    // start listening for connections
    rv = listen(sockfd, 5);
    if (rv < 0) err(1, 0);

    // main server loop, handle clients one at a time
    while (1) {
        // wait for next client, get session socket
        sa_size = sizeof(struct sockaddr_in);
        sessfd = accept(sockfd, (struct sockaddr *)&cli, &sa_size);
        if (sessfd < 0) err(1, 0);
        rv = fork();
        if (rv == 0) { // child process
            close(sockfd);
            while ( (rv = recv(sessfd, buf, MAXMSGLEN, 0)) > 0) {
                fprintf(stderr,"the errno is %d\n", errno);
                buf[rv] = 0;
                char retmsg[INTSTRLEN];
                memset(retmsg, 0, INTSTRLEN);
                int dummy = 0;
                // the first character specifies the function to call
                switch (buf[0]) {
                    case 'o': // open
                        processOpen(buf, retmsg);
                        send(sessfd, retmsg, INTSTRLEN, 0);
                        break;
                    case 'c': // close
                        processClose(buf, retmsg);
                        send(sessfd, retmsg, INTSTRLEN, 0);
                        break;
                    case 'w': // write
                        processWrite(buf, retmsg, sessfd);
                        send(sessfd, retmsg, INTSTRLEN, 0);
                        break;
                    case 'r': // read
                        processRead(buf, retmsg, sessfd);
                        break;
                    case 'l': // lseek
                        processLseek(buf, retmsg);
                        send(sessfd, retmsg, INTSTRLEN, 0);
                        break;
                    case 's': // stat
                        processStat(buf, retmsg);
                        send(sessfd, retmsg, INTSTRLEN, 0);
                        break;
                    case 'u': // unlink
                        processUnlink(buf, retmsg);
                        send(sessfd, retmsg, INTSTRLEN, 0);
                        break;
                    case 'g': // getdirentries
                        processGetdirentries(buf, sessfd);
                        break;
                    case 't': // getdirtree
                        processGetdirtree(buf, sessfd);
                        break;
                    default:
                        break;
                }

                memset(buf, 0, MAXMSGLEN+1);
            }
            close(sessfd);
            exit(0);
        }
        close(sessfd);
    }
    fprintf(stderr,"server shutting down cleanly\n");
    close(sockfd);
    return 0;
}

