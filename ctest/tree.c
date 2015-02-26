#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../include/dirtree.h"

void dfs(struct dirtreenode *root, char **buf) {
    printf("name %s, num_child %d\n", root->name, root->num_subdirs);
    int len = strlen(root->name);
    *(int *)*buf = len;
    *buf += 4;
    memcpy(*buf, root->name, len);
    *buf += len;
    *(int *)*buf = root->num_subdirs;
    *buf += 4;

    int i = 0;
    while (i < root->num_subdirs) {
        dfs(root->subdirs[i], buf);
        i++;
    }
}

struct dirtreenode *rebuild(char **buf) {
    // get name len
    int len = *(int *)*buf;
    printf("the len is %d\n", len);
    *buf += 4;
    // get name
    char * name = (char *)malloc(len+1);
    memcpy(name, *buf, len);
    name[len] = 0;//terminate
    // get num_subdirs
    *buf += len;
    int num_subdirs = *(int *)*buf;
    printf("the subdirs is %d\n", num_subdirs);
    *buf += 4;
    // get childs
    int space = sizeof(struct dirtreenodei *) * num_subdirs;
    struct dirtreenode **childs = (struct dirtreenode **) malloc(space);
    struct dirtreenode *node = (struct dirtreenode *)malloc(sizeof(struct dirtreenode));
    node->name = name;
    node->num_subdirs = num_subdirs;
    node->subdirs = childs;

    int i = 0;
    while (i < num_subdirs) {
        node->subdirs[i] = rebuild(buf);
        i++;
    }
    return node;
}

void freetree(struct dirtreenode *root) {
    free(root->name);
    int i = 0;
    while (i < root->num_subdirs) {
        freetree(root->subdirs[i]);
        i++;
    }
    free(root->subdirs);
    free(root);
}
int main() {
    printf("hello tree\n");
    struct dirtreenode *root = getdirtree("/home/caesar/15440-p1/");
    char *buf = (char *)malloc(2000000);
    char *buf2 = buf;
    dfs(root, &buf);
    printf("the buf is \n%s\n", buf2);

    struct dirtreenode *root2 = rebuild(&buf2);
    char *buf33 = (char *)malloc(2000000);
    dfs(root2, &buf33);
    freetree(root2);
    //free(buf);
    //free(buf33);
    return 0;
}
