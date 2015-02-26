/******************************************************************************
 * author: haileiy@andrew.cmu.edu
 * date:   2015 / 01 / 28
 *****************************************************************************/
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <string.h>
#include <err.h>
#include <errno.h>
#include "dirtree.h"

#define MAXMSGLEN 1024
#define INTSTRLEN 16 // max length of string that an integer can convert to
#define FDOFFSET 65536
int clientfd = 0;

/******************************************************************************
 * function prototypes
 *****************************************************************************/

void connecttcp(char *msg, char *retmsg, int msglen, int retmsglen);
void write_connecttcp(char *msg, char *retmsg, int fd, size_t count);
int (*orig_open)(const char *pathname, int flags, ...);
int (*orig_close)(int fd);
ssize_t (*orig_read)(int fd, void *buf, size_t count);
ssize_t (*orig_write)(int fd, const void *buf, size_t count);
off_t (*orig_lseek)(int fd, off_t offset, int whence);
int (*orig_xstat)(int version, const char *path, struct stat *buf);
int (*orig_unlink)(const char *pathname);
ssize_t (*orig_getdirentries)(int fd, char *buf, size_t nbytes, off_t *basep);
struct dirtreenode* (*orig_getdirtree)( const char *path );
void (*orig_freedirtree)   ( struct dirtreenode* dt );

/******************************************************************************
 * functions. all the functions will marshall the parameters into a single
 * message, send to server, get a reture message, and unmarshall return values
 * and errno. Note that the creation and teardown of tcp connection is done in
 * _init and _fini
 *****************************************************************************/

/******************************************************************************
 * rebuild a tree from a serialized string
 *****************************************************************************/

struct dirtreenode *rebuild(char **buf) {
    // get name len
    int namelen = *(int *)*buf;
    *buf += sizeof(int);
    // get name
    char * name = (char *)malloc(namelen+1);
    memcpy(name, *buf, namelen);
    name[namelen] = 0;//terminate name string
    // get num_subdirs
    *buf += namelen;
    int num_subdirs = *(int *)*buf;
    *buf += sizeof(int);
    // get childs
    struct dirtreenode **subdirs = (struct dirtreenode **) malloc(sizeof(struct dirtreenodei *) * num_subdirs);
    struct dirtreenode *node = (struct dirtreenode *)malloc(sizeof(struct dirtreenode));
    node->name = name;
    node->num_subdirs = num_subdirs;
    node->subdirs = subdirs;
    // recursively rebuild sub directory
    int i = 0;
    while (i < num_subdirs) {
        node->subdirs[i] = rebuild(buf);
        i++;
    }
    return node;
}

/******************************************************************************
 * open will marshall parameters into a single message, send it to server, get
 * the reply, unmarshall the retmsg, set local errno and return the retval
 *
 * msg format: [messagetype][filepath][flag][mode]
 * retmsg format: [return value][errno]
 *****************************************************************************/

int open(const char *pathname, int flags, ...) {
    fprintf(stderr, "client called open. flags = %d\n", flags);
    // get m
    mode_t m = 0;
    if (flags & O_CREAT) {
        va_list a;
        va_start(a, flags);
        m = va_arg(a, mode_t);
        va_end(a);
    }
    // marshall.  format: o [filepath] [flag] [mode]
    char msg[MAXMSGLEN];
    memset(msg, 0, MAXMSGLEN);
    int idx = 0;
    msg[idx++] = 'o';               // messagetype
    *(int *)(msg + idx) = flags;    // flags
    idx += sizeof(int);
    *(mode_t *)(msg + idx) = m;     // m
    idx += sizeof(mode_t);
    strcpy(msg + idx, pathname);    // pathname

    // allocate space for retmsg
    char retmsg[INTSTRLEN];
    memset(retmsg, 0, INTSTRLEN);
    int msglen = 5 + sizeof(mode_t) + strlen(pathname);
    connecttcp(msg, retmsg, msglen, INTSTRLEN);

    // unmarshall. format: [retval][errno]
    int retidx = 0;
    int rv = *(int *)(retmsg + retidx);
    retidx += sizeof(int);
    errno = *(int *)(retmsg + retidx);
    fprintf(stderr, "open: the rv is %d, errno is %d\n", rv, errno);
    if (rv < 0) return rv;
    else return rv + FDOFFSET;
}

/******************************************************************************
 * close will marshall the parameters into a single message, send it to
 * server, get retmsg from server, unmarshall the retmsg, set local errno and
 * return the retval
 *
 * msg format: [messagetype][fd]
 * retmsg format: [return value][errno]
 *****************************************************************************/

int close(int fd) {
    fprintf(stderr, "client called close. fd = %d\n", fd);
    if (fd > FDOFFSET) fd -= FDOFFSET;
    else return orig_close(fd);
    // marshall. format: o[fd]
    char msg[MAXMSGLEN];
    memset(msg, 0, MAXMSGLEN);
    int idx = 0;
    msg[idx++] = 'c';
    *(int *)(msg + idx) = fd;

    // allocate space for retmsg
    char retmsg[INTSTRLEN];
    memset(retmsg, 0, INTSTRLEN);
    int msglen = 5;// for close, the message length is fixed
    connecttcp(msg, retmsg, msglen, INTSTRLEN);

    // unmarshall. format: [retval][errno]
    int retidx = 0;
    int rv = *(int *)(retmsg + retidx);
    retidx += sizeof(int);
    errno = *(int *)(retmsg + retidx);
    fprintf(stderr, "close: the rv is %d, errno is %d\n", rv, errno);
    return rv;
}

/******************************************************************************
 * read will marshall the parameters into a single message, send it to
 * server, get retmsg from server, unmarshall the retmsg, set local errno, copy
 * the content into buf, and return the retval
 *
 * msg format: [messagetype][fd][count]
 * retmsg format: [return value][errno][content]
 *****************************************************************************/

ssize_t read(int fd, void *buf, size_t count) {
    fprintf(stderr, "client called read. fd = %d, count = %d\n", fd, count);
    if (fd > FDOFFSET) fd -= FDOFFSET;
    else return orig_read(fd, buf, count);
    // marshall. format: r[fd][count]
    char msg[INTSTRLEN];
    int idx = 0;
    msg[idx++] = 'r';
    *(int *)(msg + idx) = fd;
    idx += sizeof(int);
    *(size_t *)(msg + idx) = count;

    // dynamically allocate space for retmsg
    int retmsglen = count + sizeof(ssize_t) + sizeof(int);
    char *retmsg = (char *)malloc(retmsglen + 1);
    connecttcp(msg, retmsg, INTSTRLEN, retmsglen);

    // unmarshall. format: [retval][errno][buf]
    ssize_t rv = *(ssize_t *)retmsg;
    errno = *(int *)(retmsg + sizeof(ssize_t));
    if (rv >= 0)
    {
        memcpy(buf, (retmsg + sizeof(ssize_t) + sizeof(int)), rv);
    }
    free(retmsg);
    fprintf(stderr, "read: the rv is %d, errno is %d\n", rv, errno);
    return rv;
}

/******************************************************************************
 * write will marshall parameters into a msg, send it to server, get retmsg,
 * set local errno and return retval
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

ssize_t write(int fd, const void *buf, size_t count) {
    fprintf(stderr, "client called write. fd = %d, count = %d\n", fd, count);
    if (fd > FDOFFSET) fd -= FDOFFSET;
    else return orig_write(fd, buf, count);
    // allocate space for retmsg
    char retmsg[INTSTRLEN];
    memset(retmsg, 0, INTSTRLEN);
    write_connecttcp((char *)buf, retmsg, fd, count);

    ssize_t rv = *(ssize_t *)retmsg;// return value
    errno = *(int *)(retmsg + sizeof(ssize_t));// errno
    fprintf(stderr, "write: the rv is %d, errno is %d\n", rv, errno);
    return rv;
}

/******************************************************************************
 * lseek will marshall parameters into a msg, send it to server, get retmsg,
 * set local errno and return retval
 *
 * msg format: [messagetype][fd][count][whence]
 * retmsg format: [return value][errno]
 *****************************************************************************/


off_t lseek(int fd, off_t offset, int whence) {
    fprintf(stderr, "client called lseek. fd = %d, offset = %d, whence = %d\n", fd, offset, whence);
    if (fd > FDOFFSET) fd -= FDOFFSET;
    else return orig_lseek(fd, offset, whence);
    // marshall.
    char msg[MAXMSGLEN];
    memset(msg, 0, MAXMSGLEN);
    int idx = 0;
    msg[idx++] = 'l';
    *(int *)(msg + idx) = fd;
    idx += sizeof(int);
    *(off_t *)(msg + idx) = offset;
    idx += sizeof(off_t);
    *(int *)(msg + idx) = whence;
    idx += sizeof(int);

    // allocate space for retmsg
    char retmsg[INTSTRLEN];
    memset(retmsg, 0, INTSTRLEN);
    int msglen = sizeof(int) + sizeof(int) + sizeof(off_t);
    connecttcp(msg, retmsg, msglen, INTSTRLEN);
    // unmarshall
    off_t rv = *(int *)retmsg;
    errno = *(int *)(retmsg + sizeof(off_t));
    fprintf(stderr, "lseek: the rv is %d, errno is %d\n", rv, errno);
    return rv;
}

/******************************************************************************
 * __xstat will marshall parameters into a msg, send it to server, get retmsg,
 * set local errno and return retval
 *
 * msg format: [messagetype][version][path strlen][path]
 * retmsg format: [retval][errno][buf]
 *****************************************************************************/

int __xstat(int version, const char *path, struct stat *buf) {
    fprintf(stderr, "client called stat. version = %d\n", version);
    // marshall
    char msg[MAXMSGLEN];
    memset(msg, 0, MAXMSGLEN);
    int idx = 0;
    msg[idx++] = 's';
    *(int *)(msg + idx) = version;
    idx += sizeof(int);
    *(int *)(msg + idx) = strlen(path);
    idx += sizeof(int);
    memcpy(msg+idx, path, strlen(path));

    int msglen = 1 + sizeof(int) + sizeof(int) + strlen(path);
    // allocate space for retmsg
    char retmsg[MAXMSGLEN];
    memset(retmsg, 0, MAXMSGLEN);

    connecttcp(msg, retmsg, MAXMSGLEN, MAXMSGLEN);
    fprintf(stderr, "stat:: get msg bak\n");
    // unmarshall
    int rv = *(int *)retmsg;
    errno = *(int *)(retmsg + sizeof(int));
    *buf = *(struct stat *)(retmsg + sizeof(int) + sizeof(int));
    fprintf(stderr, "stat: rv is %d, errno is %d\n", rv, errno);
    return rv;
}

/******************************************************************************
 * unlink will marshall parameters into a msg, send it to server, get retmsg,
 * set local errno and return retval
 *
 * msg format: [messagetype][pathname length][pathname]
 * retmsg format: [retval][errno]
 *****************************************************************************/

int unlink(const char *pathname) {
    fprintf(stderr, "client called unlink\n");
    char msg[MAXMSGLEN];
    memset(msg, 0, MAXMSGLEN);
    int idx = 0;
    msg[idx++] = 'u';
    int count = strlen(pathname);
    *(int *)(msg + idx) = count;
    idx += sizeof(int);
    memcpy(msg+idx, pathname, count);

    char retmsg[INTSTRLEN];
    memset(retmsg, 0, INTSTRLEN);
    int msglen = 1 + 4 + count;
    connecttcp(msg, retmsg, msglen, INTSTRLEN);

    int retidx = 0;
    int rv = *(int *)retmsg;
    retidx += sizeof(int);
    errno = *(int *)(retmsg + retidx);
    fprintf(stderr, "unlink: rv is %d, errno = %d\n",rv, errno);
    return rv;
}

/******************************************************************************
 *
 * msg format: [messagetype][fd][nbytes]
 * retmsg format: [retval][errno][buf][basep]
 *****************************************************************************/

ssize_t getdirentries(int fd, char *buf, size_t nbytes , off_t *basep) {
    fprintf(stderr, "client called getdirentries. fd = %d, nbytes = %d\n", fd, nbytes);
    if (fd > FDOFFSET) fd -= FDOFFSET;
    else return orig_getdirentries(fd, buf, nbytes, basep);
    char msg[INTSTRLEN];
    memset(msg, 0, INTSTRLEN);
    int idx = 0;
    msg[idx++] = 'g';
    *(int *)(msg + idx) = fd;
    idx += sizeof(int);
    *(size_t *)(msg + idx) = nbytes;
    idx += sizeof(size_t);

    int retmsglen = nbytes + sizeof(ssize_t) + sizeof(int) + sizeof(off_t);
    char *retmsg = (char *)malloc(retmsglen);

    connecttcp(msg, retmsg, INTSTRLEN, retmsglen);

    int retidx = 0;
    ssize_t rv = *(ssize_t *)(retmsg + retidx);
    retidx += sizeof(ssize_t);
    errno = *(int *)(retmsg + retidx);
    retidx += sizeof(int);
    *basep = *(off_t *)(retmsg + retidx);//TODO
    retidx += sizeof(off_t);
    memcpy(buf, (retmsg + retidx), nbytes);
    fprintf(stderr, "getdirentries: rv is %d, errno = %d\n", rv, errno);
    free(retmsg);
    return rv;
}

/******************************************************************************
 * receive retmsg from server, rebuild tree from serialized string
 *
 * msg format: [messagetype][path length][path]
 * retmsg format: [retval][errno][buf]
 *****************************************************************************/

struct dirtreenode* getdirtree( const char *path ) {
    fprintf(stderr, "client:: called getdirtree\n");
    char msg[MAXMSGLEN];
    memset(msg, 0, MAXMSGLEN);
    // marshall. format: t[pathlen][path]
    int idx = 0;
    msg[idx++] = 't';
    *(int *)(msg + idx) = strlen(path);
    idx += sizeof(int);
    memcpy(msg + idx, path, strlen(path));

    char *retmsg = (char *)malloc(MAXMSGLEN);
    memset(retmsg, 0, MAXMSGLEN);

    connecttcp(msg, retmsg, MAXMSGLEN, MAXMSGLEN);

    // unmarshall. format: ...
    errno = *(int *)retmsg;
    char *p = retmsg + sizeof(int);
    struct dirtreenode *root = rebuild(&p);
    free(retmsg);
    return root;
}

/******************************************************************************
 * freedirtree doesn't need to be a rpc call
 *****************************************************************************/

void freedirtree (struct dirtreenode* dt) {
    orig_freedirtree(dt);
}

/******************************************************************************
 * This function is automatically called when program is started.
 * _init will read server ip and port from environment variable, establish a
 * tcp connect to the server.
 *****************************************************************************/

void _init(void) {
    // set function pointer orig_open to point to the original open function
    orig_open = dlsym(RTLD_NEXT, "open");
    orig_close = dlsym(RTLD_NEXT, "close");
    orig_read = dlsym(RTLD_NEXT, "read");
    orig_write = dlsym(RTLD_NEXT, "write");
    orig_lseek = dlsym(RTLD_NEXT, "lseek");
    orig_xstat = dlsym(RTLD_NEXT, "__xstat");
    orig_unlink = dlsym(RTLD_NEXT, "unlink");
    orig_getdirentries = dlsym(RTLD_NEXT, "getdirentries");
    orig_getdirtree = dlsym(RTLD_NEXT, "getdirtree");
    orig_freedirtree = dlsym(RTLD_NEXT, "freedirtree");
    // connect to server
    char *serverip;
    char *serverport;
    unsigned short port;
    int sockfd, rv;
    struct sockaddr_in srv;
    // Get environment variable indicating the ip address of the server
    serverip = getenv("server15440");
    if (!serverip) serverip = "127.0.0.1";
    // Get environment variable indicating the port of the server
    serverport = getenv("serverport15440");
    if (!serverport) serverport = "15440";
    port = (unsigned short)atoi(serverport);
    // Create socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) err(1, 0);
    // setup address structure to point to server
    memset(&srv, 0, sizeof(srv));
    srv.sin_family = AF_INET;
    srv.sin_addr.s_addr = inet_addr(serverip);
    srv.sin_port = htons(port);
    // actually connect to the server
    rv = connect(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
    if (rv < 0) err(1, 0);
    clientfd = sockfd;
}

/******************************************************************************
 * This function is automatically called when program finishes.
 * _fini will close the socket that _init establishes
 *****************************************************************************/

void _fini(void) {
    // teardown the socket
    orig_close(clientfd);
}

/******************************************************************************
 * write_connecttcp is special. Only write will call it. It takes fd, count
 * as parameter.

 * 1:  client send fd and count to server. The server dynamically allocates a
 *     buffer to store the 3rd message
 * 2:  server sends a short ack back to client
 * 3:  upon receiving ack, the client sends the content to write to server
 * 4:  server receives content, write it to file, then marshall the retval and
 *     errno to retmsg, which will be sent back to client
 *
 *****************************************************************************/
void write_connecttcp(char* msg, char *retmsg, int fd, size_t count) {
    char msg1[INTSTRLEN];
    char retmsg1[INTSTRLEN];
    memset(msg1, 0, INTSTRLEN);
    memset(retmsg1, 0, INTSTRLEN);
    int rv = 0;
    // send a message to notify the server of fd, count
    int idx = 0;
    msg1[idx] = 'w';
    idx ++;
    *(size_t *)(msg1 + idx) = count;
    idx += sizeof(size_t);
    *(int *)(msg1 + idx) = fd;

    send(clientfd, msg1, INTSTRLEN, 0);
    // receive ack from server
    rv = recv(clientfd, retmsg1, INTSTRLEN, 0);
    if (rv < 0) err(1, 0);
    // send the buffer
    send(clientfd, msg, count, 0);
    // get retmsg
    rv = recv(clientfd, retmsg, INTSTRLEN, 0);
    if (rv < 0) err(1, 0);         // in case something went wrong
}

/******************************************************************************
 * connecttcp takes msg, retmsg, msglen, retmsglen as parameters.
 * It basically sends msg to server, get retmsg back.
 *****************************************************************************/

void connecttcp(char* msg, char *retmsg, int msglen, int retmsglen) {
    // send message to server
    send(clientfd, msg, msglen, 0);
    // get message back
    int sum_cnt = 0;
    char tmpbuf[MAXMSGLEN];
    memset(tmpbuf, 0, MAXMSGLEN);

    while (sum_cnt < retmsglen)
    {
        int part_cnt = recv(clientfd, tmpbuf, MAXMSGLEN, 0);
        memcpy(retmsg+sum_cnt, tmpbuf, part_cnt);
        sum_cnt += part_cnt;
    }
}


