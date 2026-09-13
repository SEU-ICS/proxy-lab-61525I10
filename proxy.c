#include "csapp.h"
#include <strings.h>

#define MAXLINE 8192
#define MAX_CACHE_SIZE 1049000
#define MAX_OBJECT_SIZE 102400
#define NTHREADS 8
#define SBUFSIZE 16

/* ---------- 缓存相关 ---------- */
typedef struct cache_block {
    char *url;
    char *data;
    size_t size;
    struct cache_block *prev;
    struct cache_block *next;
} cache_block_t;

cache_block_t *cache_head = NULL;
cache_block_t *cache_tail = NULL;
size_t cache_total = 0;
pthread_mutex_t cache_mutex = PTHREAD_MUTEX_INITIALIZER;

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
int cache_get(const char *url, char **data, size_t *size);
void cache_put(const char *url, const char *data, size_t size);

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

/* ---------- 缓存实现 ---------- */
int cache_get(const char *url, char **data, size_t *size) {
    pthread_mutex_lock(&cache_mutex);
    cache_block_t *p = cache_head;
    while (p) {
        if (strcmp(p->url, url) == 0) {
            /* 移到头部（LRU 更新） */
            if (p != cache_head) {
                if (p->prev) p->prev->next = p->next;
                if (p->next) p->next->prev = p->prev;
                if (p == cache_tail) cache_tail = p->prev;
                p->prev = NULL;
                p->next = cache_head;
                if (cache_head) cache_head->prev = p;
                cache_head = p;
                if (!cache_tail) cache_tail = p;
            }
            *data = Malloc(p->size);
            memcpy(*data, p->data, p->size);
            *size = p->size;
            pthread_mutex_unlock(&cache_mutex);
            return 1;
        }
        p = p->next;
    }
    pthread_mutex_unlock(&cache_mutex);
    return 0;
}

void cache_put(const char *url, const char *data, size_t size) {
    if (size > MAX_OBJECT_SIZE || size == 0) return;

    pthread_mutex_lock(&cache_mutex);

    /* 如果已存在，删除旧块 */
    cache_block_t *p = cache_head;
    while (p) {
        if (strcmp(p->url, url) == 0) {
            if (p->prev) p->prev->next = p->next;
            if (p->next) p->next->prev = p->prev;
            if (p == cache_head) cache_head = p->next;
            if (p == cache_tail) cache_tail = p->prev;
            cache_total -= p->size;
            free(p->url);
            free(p->data);
            free(p);
            break;
        }
        p = p->next;
    }

    /* 驱逐直到空间足够 */
    while (cache_total + size > MAX_CACHE_SIZE && cache_tail) {
        cache_block_t *old = cache_tail;
        cache_tail = old->prev;
        if (cache_tail) cache_tail->next = NULL;
        else cache_head = NULL;
        cache_total -= old->size;
        free(old->url);
        free(old->data);
        free(old);
    }

    /* 插入新块到头部 */
    cache_block_t *new_block = Malloc(sizeof(cache_block_t));
    new_block->url = Malloc(strlen(url) + 1);
    strcpy(new_block->url, url);
    new_block->data = Malloc(size);
    memcpy(new_block->data, data, size);
    new_block->size = size;
    new_block->prev = NULL;
    new_block->next = cache_head;
    if (cache_head) cache_head->prev = new_block;
    cache_head = new_block;
    if (!cache_tail) cache_tail = new_block;
    cache_total += size;

    pthread_mutex_unlock(&cache_mutex);
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

/* ---------- 核心代理逻辑 ---------- */
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

    /* 解析 host:port，端口保存为字符串 */
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

    /* 构造缓存 key（缓冲区开大，消除截断警告） */
    char url_key[2 * MAXLINE + 128];
    snprintf(url_key, sizeof(url_key), "http://%s:%s%s",
             host, portstr_buf, path);

    /* 查询缓存 */
    char *cached_data;
    size_t cached_size;
    if (cache_get(url_key, &cached_data, &cached_size)) {
        Rio_writen(fd, cached_data, cached_size);
        free(cached_data);
        return;
    }

    /* 连接目标服务器 */
    int serverfd = Open_clientfd(host, portstr_buf);
    if (serverfd < 0) {
        clienterror(fd, host, "502", "Bad Gateway",
                    "Cannot connect to server");
        return;
    }

    /* 构造并发送转发请求（缓冲区同样开大） */
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

    /* 读取服务器响应并转发，同时尝试缓存 */
    rio_t server_rio;
    Rio_readinitb(&server_rio, serverfd);

    char *response_buf = Malloc(MAX_OBJECT_SIZE);
    size_t total = 0;

    /* 转发响应头 */
    while (Rio_readlineb(&server_rio, buf, MAXLINE) > 0) {
        Rio_writen(fd, buf, strlen(buf));
        size_t len = strlen(buf);
        if (total + len <= MAX_OBJECT_SIZE) {
            memcpy(response_buf + total, buf, len);
            total += len;
        }
        if (strcmp(buf, "\r\n") == 0) break;
    }

    /* 转发响应体 */
    ssize_t n;
    while ((n = Rio_readnb(&server_rio, buf, MAXLINE)) > 0) {
        Rio_writen(fd, buf, n);
        if (total + n <= MAX_OBJECT_SIZE) {
            memcpy(response_buf + total, buf, n);
            total += n;
        }
    }

    /* 存入缓存 */
    if (total <= MAX_OBJECT_SIZE && total > 0) {
        cache_put(url_key, response_buf, total);
    }

    free(response_buf);
    Close(serverfd);
}