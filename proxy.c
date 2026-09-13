#include "csapp.h"
#include <strings.h>

#define MAXLINE 8192
#define NTHREADS 8
#define SBUFSIZE 16

/* ---------- 有界缓冲区 ---------- */
typedef struct {
    int *buf;
    int n;
    int front;
    int rear;
    sem_t mutex;
    sem_t slots;
    sem_t items;
} sbuf_t;

sbuf_t sbuf;

/* ---------- 函数声明 ---------- */
void doit(int fd);
void clienterror(int fd, char *cause, char *errnum,
                 char *shortmsg, char *longmsg);
void *thread(void *vargp);
void sbuf_init(sbuf_t *sp, int n);
void sbuf_insert(sbuf_t *sp, int item);
int sbuf_remove(sbuf_t *sp);

/* ---------- main ---------- */
int main(int argc, char **argv) {
    int listenfd, connfd;
    socklen_t clientlen;
    struct sockaddr_storage clientaddr;
    pthread_t tid;

    if (argc != 2) {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }

    Signal(SIGPIPE, SIG_IGN);
    listenfd = Open_listenfd(argv[1]);

    sbuf_init(&sbuf, SBUFSIZE);

    for (int i = 0; i < NTHREADS; i++) {
        Pthread_create(&tid, NULL, thread, NULL);
    }

    while (1) {
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, &clientlen);
        sbuf_insert(&sbuf, connfd);
    }
}

/* ---------- 工作线程 ---------- */
void *thread(void *vargp) {
    Pthread_detach(Pthread_self());
    while (1) {
        int connfd = sbuf_remove(&sbuf);
        doit(connfd);
        Close(connfd);
    }
    return NULL;
}

/* ---------- 有界缓冲区实现 ---------- */
void sbuf_init(sbuf_t *sp, int n) {
    sp->buf = Calloc(n, sizeof(int));
    sp->n = n;
    sp->front = sp->rear = 0;
    Sem_init(&sp->mutex, 0, 1);
    Sem_init(&sp->slots, 0, n);
    Sem_init(&sp->items, 0, 0);
}

void sbuf_insert(sbuf_t *sp, int item) {
    P(&sp->slots);
    P(&sp->mutex);
    sp->buf[(++sp->rear) % (sp->n)] = item;
    V(&sp->mutex);
    V(&sp->items);
}

int sbuf_remove(sbuf_t *sp) {
    int item;
    P(&sp->items);
    P(&sp->mutex);
    item = sp->buf[(++sp->front) % (sp->n)];
    V(&sp->mutex);
    V(&sp->slots);
    return item;
}

/* ---------- 错误响应 ---------- */
void clienterror(int fd, char *cause, char *errnum,
                 char *shortmsg, char *longmsg) {
    char buf[MAXLINE], body[MAXLINE];

    sprintf(body, "<html><title>Proxy Error</title>");
    sprintf(body + strlen(body),
            "<body bgcolor=\"ffffff\">\r\n");
    sprintf(body + strlen(body),
            "%s: %s\r\n", errnum, shortmsg);
    sprintf(body + strlen(body),
            "<p>%s: %s\r\n", longmsg, cause);
    sprintf(body + strlen(body),
            "<hr><em>Proxy Lab</em>\r\n");

    sprintf(buf, "HTTP/1.0 %s %s\r\n", errnum, shortmsg);
    Rio_writen(fd, buf, strlen(buf));
    sprintf(buf, "Content-type: text/html\r\n");
    Rio_writen(fd, buf, strlen(buf));
    sprintf(buf, "Content-length: %d\r\n\r\n", (int)strlen(body));
    Rio_writen(fd, buf, strlen(buf));
    Rio_writen(fd, body, strlen(body));
}

/* ---------- 核心代理逻辑（有并发，无缓存） ---------- */
void doit(int fd) {
    char buf[MAXLINE], method[MAXLINE], url[MAXLINE], version[MAXLINE];
    char host[MAXLINE], path[MAXLINE];
    char portstr_buf[16];
    rio_t rio;

    Rio_readinitb(&rio, fd);
    if (!Rio_readlineb(&rio, buf, MAXLINE)) return;
    sscanf(buf, "%s %s %s", method, url, version);

    if (strcasecmp(method, "GET")) {
        clienterror(fd, method, "501", "Not Implemented",
                    "Proxy only supports GET");
        return;
    }

    /* 读取并丢弃剩余请求头 */
    while (strcmp(buf, "\r\n")) {
        Rio_readlineb(&rio, buf, MAXLINE);
    }

    /* 解析 URL */
    if (strncasecmp(url, "http://", 7) != 0) {
        clienterror(fd, url, "400", "Bad Request", "Invalid URL");
        return;
    }

    char *hostbegin = url + 7;
    char *pathbegin = strchr(hostbegin, '/');
    if (pathbegin) {
        strcpy(path, pathbegin);
        *pathbegin = '\0';
    } else {
        strcpy(path, "/");
    }

    char *portstr = strchr(hostbegin, ':');
    if (portstr) {
        *portstr = '\0';
        strncpy(portstr_buf, portstr + 1, sizeof(portstr_buf) - 1);
        portstr_buf[sizeof(portstr_buf) - 1] = '\0';
    } else {
        strcpy(portstr_buf, "80");
    }
    strncpy(host, hostbegin, MAXLINE - 1);
    host[MAXLINE - 1] = '\0';

    /* 直接连目标服务器，不查缓存 */
    int serverfd = Open_clientfd(host, portstr_buf);
    if (serverfd < 0) {
        clienterror(fd, host, "502", "Bad Gateway",
                    "Cannot connect to server");
        return;
    }

    /* 构造并发送转发请求 */
    char req[2 * MAXLINE + 256];
    snprintf(req, sizeof(req),
             "GET %s HTTP/1.0\r\n"
             "Host: %s\r\n"
             "User-Agent: proxy-lab\r\n"
             "Connection: close\r\n"
             "Proxy-Connection: close\r\n"
             "\r\n",
             path, host);
    Rio_writen(serverfd, req, strlen(req));

    /* 读取服务器响应并转发 */
    rio_t server_rio;
    Rio_readinitb(&server_rio, serverfd);

    /* 转发响应头 */
    while (Rio_readlineb(&server_rio, buf, MAXLINE) > 0) {
        Rio_writen(fd, buf, strlen(buf));
        if (strcmp(buf, "\r\n") == 0) break;
    }

    /* 转发响应体 */
    ssize_t n;
    while ((n = Rio_readnb(&server_rio, buf, MAXLINE)) > 0) {
        Rio_writen(fd, buf, n);
    }

    Close(serverfd);
}