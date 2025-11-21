// storage_server.c
// Compile: gcc -o storage_server storage_server.c -lpthread
// Storage Server: listens on 9100, registers to NM at 127.0.0.1:9000

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <dirent.h>

#define SS_PORT 9100
#define NM_IP "127.0.0.1"
#define NM_PORT 9000
#define STORAGE_DIR "./storage"
#define BUFFER_SIZE 8192
#define FNAME_LEN 256
#define USERNAME_LEN 64
#define PATH_LEN 512
#define END_MARKER "<END>"
#define LOG_FILE "./ss_log.txt"

typedef struct {
    char filename[FNAME_LEN];
    char owner[USERNAME_LEN];
    int sentence_count;
    pthread_mutex_t *sentence_locks;
    char undo_path[PATH_LEN];
    char last_access[64];
    int word_count;
    int char_count;
} FileMeta;

/* globals */
static FileMeta *files = NULL;
static int file_count = 0;
static pthread_mutex_t files_mutex = PTHREAD_MUTEX_INITIALIZER;
static FILE *log_fp = NULL;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

/* logging */
static void log_message(const char *level, const char *client_ip, int client_port, const char *msg) {
    pthread_mutex_lock(&log_mutex);
    if (!log_fp) log_fp = fopen(LOG_FILE, "a");
    if (log_fp) {
        time_t now = time(NULL);
        char timebuf[64];
        struct tm tmv;
        localtime_r(&now, &tmv);
        strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", &tmv);
        fprintf(log_fp, "[%s] [%s] [%s:%d] %s\n", timebuf, level, client_ip, client_port, msg);
        fflush(log_fp);
    }
    pthread_mutex_unlock(&log_mutex);
}

/* helpers */
static void ensure_storage_dir() {
    struct stat st;
    if (stat(STORAGE_DIR, &st) == -1) mkdir(STORAGE_DIR, 0777);
}

static void get_path(const char *name, char *out, size_t n) {
    snprintf(out, n, "%s/%s", STORAGE_DIR, name);
}

static void timestamp(char *buf, size_t n) {
    time_t t = time(NULL);
    struct tm tmv;
    localtime_r(&t, &tmv);
    strftime(buf, n, "%Y-%m-%d %H:%M:%S", &tmv);
}

static char *read_whole(const char *name) {
    char path[PATH_LEN];
    get_path(name, path, sizeof(path));
    FILE *f = fopen(path, "r");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }
    long sz = ftell(f);
    if (sz < 0) {
        fclose(f);
        return NULL;
    }
    rewind(f);
    char *buf = malloc((size_t)sz + 1);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    size_t r = fread(buf, 1, (size_t)sz, f);
    buf[r] = '\0';
    fclose(f);
    return buf;
}

static int atomic_write(const char *name, const char *content) {
    char path[PATH_LEN], tmp[PATH_LEN + 32];
    get_path(name, path, sizeof(path));
    snprintf(tmp, sizeof(tmp), "%s.tmpXXXXXX", path);
    int fd = mkstemp(tmp);
    if (fd < 0) return -1;
    ssize_t w = write(fd, content, strlen(content));
    close(fd);
    if (w < 0) {
        unlink(tmp);
        return -1;
    }
    if (rename(tmp, path) != 0) {
        unlink(tmp);
        return -1;
    }
    return 0;
}

static int count_sentences(const char *s) {
    int c = 0;
    if (!s) return 1;
    for (const char *p = s; *p; ++p)
        if (*p == '.' || *p == '?' || *p == '!')
            c++;
    return c > 0 ? c : 1;
}

static int count_words(const char *s) {
    int cnt = 0, in = 0;
    if (!s) return 0;
    for (const char *p = s; *p; ++p) {
        if (*p == ' ' || *p == '\n' || *p == '\t' || *p == '\r') {
            if (in) {
                cnt++;
                in = 0;
            }
        } else
            in = 1;
    }
    if (in) cnt++;
    return cnt;
}

/* meta helpers */
static int find_meta(const char *name) {
    pthread_mutex_lock(&files_mutex);
    for (int i = 0; i < file_count; ++i)
        if (strcmp(files[i].filename, name) == 0) {
            pthread_mutex_unlock(&files_mutex);
            return i;
        }
    pthread_mutex_unlock(&files_mutex);
    return -1;
}

static int create_meta_if_missing(const char *name, const char *owner) {
    int idx = find_meta(name);
    if (idx >= 0) return idx;
    
    pthread_mutex_lock(&files_mutex);
    FileMeta *n = realloc(files, sizeof(FileMeta) * (file_count + 1));
    if (!n) {
        pthread_mutex_unlock(&files_mutex);
        return -1;
    }
    files = n;
    FileMeta *m = &files[file_count];
    memset(m, 0, sizeof(*m));
    strncpy(m->filename, name, FNAME_LEN - 1);
    strncpy(m->owner, owner ? owner : "unknown", USERNAME_LEN - 1);
    m->sentence_count = 1;
    m->sentence_locks = calloc(1, sizeof(pthread_mutex_t));
    pthread_mutex_init(&m->sentence_locks[0], NULL);
    m->undo_path[0] = '\0';
    timestamp(m->last_access, sizeof(m->last_access));
    m->word_count = 0;
    m->char_count = 0;
    int out = file_count++;
    pthread_mutex_unlock(&files_mutex);
    return out;
}

static void save_undo_copy(const char *name, FileMeta *m) {
    char undo[PATH_LEN];
    snprintf(undo, sizeof(undo), "%s/.%s.undo", STORAGE_DIR, name);
    char *cur = read_whole(name);
    if (!cur) {
        m->undo_path[0] = '\0';
        return;
    }
    FILE *f = fopen(undo, "w");
    if (!f) {
        free(cur);
        m->undo_path[0] = '\0';
        return;
    }
    fputs(cur, f);
    fclose(f);
    free(cur);
    strncpy(m->undo_path, undo, PATH_LEN - 1);
}

/* replace sentence sidx with new text */
static char *replace_sentence(const char *content, int sidx, const char *new_sentence) {
    char *buf = strdup(content ? content : "");
    if (!buf) return NULL;
    
    char **sent = NULL;
    int sc = 0;
    size_t off = 0, L = strlen(buf);
    
    while (off < L) {
        size_t i = off;
        while (i < L && buf[i] != '.' && buf[i] != '?' && buf[i] != '!')
            i++;
        if (i < L) {
            size_t len = i - off + 1;
            char *s = malloc(len + 1);
            memcpy(s, buf + off, len);
            s[len] = '\0';
            sent = realloc(sent, sizeof(char *) * (sc + 1));
            sent[sc++] = s;
            off = i + 1;
        } else {
            size_t len = L - off;
            if (len > 0) {
                char *s = malloc(len + 2);
                memcpy(s, buf + off, len);
                s[len] = '\0';
                sent = realloc(sent, sizeof(char *) * (sc + 1));
                sent[sc++] = s;
            }
            break;
        }
    }
    free(buf);
    
    if (sidx < 0) sidx = 0;
    if (sidx >= sc) {
        sent = realloc(sent, sizeof(char *) * (sidx + 1));
        for (int i = sc; i <= sidx; ++i)
            sent[i] = strdup("");
        sc = sidx + 1;
    }
    
    char *ns = strdup(new_sentence ? new_sentence : "");
    size_t nslen = strlen(ns);
    while (nslen > 0 && (ns[nslen - 1] == ' ' || ns[nslen - 1] == '\n' || ns[nslen - 1] == '\r'))
        ns[--nslen] = '\0';
    if (nslen > 0) {
        char last = ns[nslen - 1];
        if (!(last == '.' || last == '?' || last == '!')) {
            char *tmp = malloc(nslen + 2);
            memcpy(tmp, ns, nslen);
            tmp[nslen] = '.';
            tmp[nslen + 1] = '\0';
            free(ns);
            ns = tmp;
        }
    }
    
    free(sent[sidx]);
    sent[sidx] = ns;
    
    size_t total = 0;
    for (int i = 0; i < sc; ++i)
        total += strlen(sent[i]) + 1;
    char *out = malloc(total + 1);
    out[0] = '\0';
    for (int i = 0; i < sc; ++i) {
        strcat(out, sent[i]);
        if (i < sc - 1) strcat(out, " ");
        free(sent[i]);
    }
    free(sent);
    return out;
}

/* stream words to socket with 0.1s delay */
static void stream_words_to_sock(int sock, const char *name) {
    char *c = read_whole(name);
    if (!c) {
        send(sock, "ERROR: File not found\n", 22, 0);
        send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
        return;
    }
    char *p = c;
    while (*p) {
        while (*p && (*p == ' ' || *p == '\n' || *p == '\t' || *p == '\r'))
            p++;
        if (!*p) break;
        char tok[1024];
        int i = 0;
        while (*p && !(*p == ' ' || *p == '\n' || *p == '\t' || *p == '\r')) {
            if (i < (int)sizeof(tok) - 1)
                tok[i++] = *p;
            p++;
        }
        tok[i] = '\0';
        if (i > 0) {
            send(sock, tok, strlen(tok), 0);
            send(sock, " ", 1, 0);
            usleep(100000);
        }
    }
    free(c);
    send(sock, "\nSTOP\n", 6, 0);
    send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
}

/* handler */
static void *conn_handler(void *arg) {
    int sock = *((int *)arg);
    free(arg);
    
    struct sockaddr_in peer;
    socklen_t plen = sizeof(peer);
    getpeername(sock, (struct sockaddr *)&peer, &plen);
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &peer.sin_addr, client_ip, sizeof(client_ip));
    int client_port = ntohs(peer.sin_port);
    
    char buf[BUFFER_SIZE];
    while (1) {
        memset(buf, 0, sizeof(buf));
        ssize_t r = recv(sock, buf, sizeof(buf) - 1, 0);
        if (r <= 0) break;
        buf[r] = '\0';
        buf[strcspn(buf, "\r\n")] = 0;
        
        char logmsg[512];
        snprintf(logmsg, sizeof(logmsg), "REQUEST: %s", buf);
        log_message("INFO", client_ip, client_port, logmsg);
        
        if (strncmp(buf, "CREATE ", 7) == 0) {
            char fname[FNAME_LEN], owner[USERNAME_LEN];
            int got = sscanf(buf + 7, "%255s %63s", fname, owner);
            if (got < 1) {
                send(sock, "ERROR: CREATE requires filename\n", 32, 0);
                send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                log_message("ERROR", client_ip, client_port, "CREATE missing filename");
                continue;
            }
            if (got < 2) owner[0] = '\0';
            
            char path[PATH_LEN];
            get_path(fname, path, sizeof(path));
            FILE *f = fopen(path, "r");
            if (f) {
                fclose(f);
                send(sock, "ERROR: File already exists\n", 27, 0);
                send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                snprintf(logmsg, sizeof(logmsg), "CREATE failed: %s exists", fname);
                log_message("ERROR", client_ip, client_port, logmsg);
                continue;
            }
            
            if (atomic_write(fname, "") != 0) {
                send(sock, "ERROR: create failed\n", 21, 0);
                send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                log_message("ERROR", client_ip, client_port, "CREATE write failed");
                continue;
            }
            
            create_meta_if_missing(fname, owner);
            send(sock, "ACK: File created\n", 18, 0);
            send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
            snprintf(logmsg, sizeof(logmsg), "ACK: Created file %s", fname);
            log_message("INFO", client_ip, client_port, logmsg);
        }
        else if (strncmp(buf, "READ ", 5) == 0) {
            char fname[FNAME_LEN];
            sscanf(buf + 5, "%255s", fname);
            char *c = read_whole(fname);
            if (!c) {
                send(sock, "ERROR: File not found\n", 22, 0);
                send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                snprintf(logmsg, sizeof(logmsg), "READ failed: %s not found", fname);
                log_message("ERROR", client_ip, client_port, logmsg);
                continue;
            }
            send(sock, c, strlen(c), 0);
            send(sock, "\n", 1, 0);
            send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
            free(c);
            snprintf(logmsg, sizeof(logmsg), "ACK: Read file %s", fname);
            log_message("INFO", client_ip, client_port, logmsg);
        }
        else if (strncmp(buf, "WRITE ", 6) == 0) {
            char fname[FNAME_LEN];
            int sidx = 0;
            if (sscanf(buf + 6, "%255s %d", fname, &sidx) < 2) {
                send(sock, "ERROR: WRITE requires filename and sentence_idx\n", 48, 0);
                send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                log_message("ERROR", client_ip, client_port, "WRITE missing args");
                continue;
            }
            
            int midx = find_meta(fname);
            if (midx < 0) midx = create_meta_if_missing(fname, "unknown");
            FileMeta *m = &files[midx];
            
            if (sidx < 0) {
                send(sock, "ERROR: invalid sentence index\n", 30, 0);
                send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                log_message("ERROR", client_ip, client_port, "WRITE invalid sentence index");
                continue;
            }
            
            if (sidx >= m->sentence_count) {
                pthread_mutex_lock(&files_mutex);
                int old = m->sentence_count;
                int newc = sidx + 1;
                pthread_mutex_t *tmp = realloc(m->sentence_locks, sizeof(pthread_mutex_t) * newc);
                if (!tmp) {
                    pthread_mutex_unlock(&files_mutex);
                    send(sock, "ERROR: memory\n", 14, 0);
                    send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                    log_message("ERROR", client_ip, client_port, "WRITE memory alloc failed");
                    continue;
                }
                m->sentence_locks = tmp;
                for (int i = old; i < newc; ++i)
                    pthread_mutex_init(&m->sentence_locks[i], NULL);
                m->sentence_count = newc;
                pthread_mutex_unlock(&files_mutex);
            }
            
            pthread_mutex_lock(&m->sentence_locks[sidx]);
            save_undo_copy(fname, m);
            
            char accum[BUFFER_SIZE * 2];
            accum[0] = '\0';
            while (1) {
                memset(buf, 0, sizeof(buf));
                ssize_t b = recv(sock, buf, sizeof(buf) - 1, 0);
                if (b <= 0) break;
                buf[b] = '\0';
                buf[strcspn(buf, "\r\n")] = 0;
                if (strcmp(buf, "ETIRW") == 0) break;
                if (strlen(accum) + strlen(buf) + 2 < sizeof(accum)) {
                    if (strlen(accum) > 0) strcat(accum, " ");
                    strcat(accum, buf);
                }
            }
            
            char *cur = read_whole(fname);
            if (!cur) cur = strdup("");
            char *newcontent = replace_sentence(cur, sidx, accum);
            free(cur);
            
            if (!newcontent) {
                pthread_mutex_unlock(&m->sentence_locks[sidx]);
                send(sock, "ERROR: replace failed\n", 22, 0);
                send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                log_message("ERROR", client_ip, client_port, "WRITE replace failed");
                continue;
            }
            
            if (atomic_write(fname, newcontent) != 0) {
                free(newcontent);
                pthread_mutex_unlock(&m->sentence_locks[sidx]);
                send(sock, "ERROR: write failed\n", 20, 0);
                send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                log_message("ERROR", client_ip, client_port, "WRITE atomic write failed");
                continue;
            }
            free(newcontent);
            
            char *latest = read_whole(fname);
            if (latest) {
                m->sentence_count = count_sentences(latest);
                m->word_count = count_words(latest);
                m->char_count = strlen(latest);
                timestamp(m->last_access, sizeof(m->last_access));
                free(latest);
            }
            pthread_mutex_unlock(&m->sentence_locks[sidx]);
            
            send(sock, "ACK: Write complete\n", 20, 0);
            send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
            snprintf(logmsg, sizeof(logmsg), "ACK: Write to %s sentence %d", fname, sidx);
            log_message("INFO", client_ip, client_port, logmsg);
        }
        else if (strncmp(buf, "UNDO ", 5) == 0) {
            char fname[FNAME_LEN];
            sscanf(buf + 5, "%255s", fname);
            int midx = find_meta(fname);
            if (midx < 0) {
                send(sock, "ERROR: File not found\n", 22, 0);
                send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                snprintf(logmsg, sizeof(logmsg), "UNDO failed: %s not found", fname);
                log_message("ERROR", client_ip, client_port, logmsg);
                continue;
            }
            
            FileMeta *m = &files[midx];
            if (m->undo_path[0] == '\0') {
                send(sock, "ERROR: No undo available\n", 25, 0);
                send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                log_message("ERROR", client_ip, client_port, "UNDO no history");
                continue;
            }
            
            FILE *uf = fopen(m->undo_path, "r");
            if (!uf) {
                send(sock, "ERROR: undo read failed\n", 24, 0);
                send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                log_message("ERROR", client_ip, client_port, "UNDO read failed");
                continue;
            }
            
            fseek(uf, 0, SEEK_END);
            long sz = ftell(uf);
            rewind(uf);
            char *u = malloc(sz + 1);
            fread(u, 1, sz, uf);
            u[sz] = '\0';
            fclose(uf);
            
            if (atomic_write(fname, u) != 0) {
                free(u);
                send(sock, "ERROR: undo write failed\n", 25, 0);
                send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                log_message("ERROR", client_ip, client_port, "UNDO write failed");
                continue;
            }
            free(u);
            
            send(sock, "ACK: Undo complete\n", 19, 0);
            send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
            snprintf(logmsg, sizeof(logmsg), "ACK: Undo on %s", fname);
            log_message("INFO", client_ip, client_port, logmsg);
        }
        else if (strncmp(buf, "DELETE ", 7) == 0) {
            char fname[FNAME_LEN];
            sscanf(buf + 7, "%255s", fname);
            char path[PATH_LEN];
            get_path(fname, path, sizeof(path));
            if (unlink(path) == 0) {
                send(sock, "ACK: File deleted\n", 18, 0);
                send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                snprintf(logmsg, sizeof(logmsg), "ACK: Deleted %s", fname);
                log_message("INFO", client_ip, client_port, logmsg);
            } else {
                send(sock, "ERROR: delete failed\n", 21, 0);
                send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                snprintf(logmsg, sizeof(logmsg), "DELETE failed: %s", fname);
                log_message("ERROR", client_ip, client_port, logmsg);
            }
        }
        else if (strncmp(buf, "STREAM ", 7) == 0) {
            char fname[FNAME_LEN];
            sscanf(buf + 7, "%255s", fname);
            snprintf(logmsg, sizeof(logmsg), "STREAM: %s", fname);
            log_message("INFO", client_ip, client_port, logmsg);
            stream_words_to_sock(sock, fname);
        }
        else if (strncmp(buf, "INFO ", 5) == 0) {
            char fname[FNAME_LEN];
            sscanf(buf + 5, "%255s", fname);
            int midx = find_meta(fname);
            if (midx < 0) {
                send(sock, "ERROR: File not found\n", 22, 0);
                send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
                snprintf(logmsg, sizeof(logmsg), "INFO failed: %s not found", fname);
                log_message("ERROR", client_ip, client_port, logmsg);
                continue;
            }
            
            FileMeta *m = &files[midx];
            char out[512];
            snprintf(out, sizeof(out), "Owner:%s\nWords:%d\nChars:%d\nSentences:%d\nLastAccess:%s\n",
                     m->owner, m->word_count, m->char_count, m->sentence_count, m->last_access);
            send(sock, out, strlen(out), 0);
            send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
            snprintf(logmsg, sizeof(logmsg), "ACK: INFO for %s", fname);
            log_message("INFO", client_ip, client_port, logmsg);
        }
        else {
            send(sock, "ERROR: Unknown command\n", 23, 0);
            send(sock, END_MARKER "\n", strlen(END_MARKER) + 1, 0);
            snprintf(logmsg, sizeof(logmsg), "ERROR: Unknown command: %s", buf);
            log_message("ERROR", client_ip, client_port, logmsg);
        }
    }
    close(sock);
    return NULL;
}

/* scan storage directory and return file list */
static char **get_file_list(int *count) {
    *count = 0;
    DIR *d = opendir(STORAGE_DIR);
    if (!d) return NULL;
    
    char **list = NULL;
    struct dirent *entry;
    while ((entry = readdir(d)) != NULL) {
        if (entry->d_name[0] == '.') continue;
        if (entry->d_type == DT_REG || entry->d_type == DT_UNKNOWN) {
            list = realloc(list, sizeof(char *) * (*count + 1));
            list[*count] = strdup(entry->d_name);
            (*count)++;
        }
    }
    closedir(d);
    return list;
}

/* register with NM */
static void register_with_nm(const char *nmip, int nmport, const char *ssip, int ssport) {
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) {
        perror("socket");
        return;
    }
    
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(nmport);
    inet_pton(AF_INET, nmip, &addr.sin_addr);
    
    if (connect(s, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
        int file_cnt = 0;
        char **file_list = get_file_list(&file_cnt);
        
        char reg[4096];
        int offset = snprintf(reg, sizeof(reg), "REGISTER_SS %s %d %d", ssip, ssport, file_cnt);
        
        for (int i = 0; i < file_cnt; ++i) {
            offset += snprintf(reg + offset, sizeof(reg) - offset, " %s", file_list[i]);
            free(file_list[i]);
        }
        if (file_list) free(file_list);
        
        strcat(reg, "\n");
        send(s, reg, strlen(reg), 0);
        close(s);
        printf("[SS] Registered with NM %s:%d (%d files)\n", nmip, nmport, file_cnt);
        log_message("INFO", "127.0.0.1", 0, "Registered with NM");
    } else {
        perror("[SS] register");
        close(s);
    }
}

int main(void) {
    ensure_storage_dir();
    
    int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock < 0) {
        perror("socket");
        exit(1);
    }
    
    int opt = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in serv = {0};
    serv.sin_family = AF_INET;
    serv.sin_port = htons(SS_PORT);
    serv.sin_addr.s_addr = INADDR_ANY;
    
    if (bind(listen_sock, (struct sockaddr *)&serv, sizeof(serv)) < 0) {
        perror("bind");
        exit(1);
    }
    
    if (listen(listen_sock, 50) < 0) {
        perror("listen");
        exit(1);
    }
    
    register_with_nm(NM_IP, NM_PORT, "10.14.66.6", SS_PORT);
    printf("[SS] Listening on %d\n", SS_PORT);
    
    while (1) {
        int *cs = malloc(sizeof(int));
        struct sockaddr_in cli;
        socklen_t alen = sizeof(cli);
        *cs = accept(listen_sock, (struct sockaddr *)&cli, &alen);
        if (*cs < 0) {
            free(cs);
            continue;
        }
        pthread_t tid;
        pthread_create(&tid, NULL, conn_handler, cs);
        pthread_detach(tid);
    }
    
    close(listen_sock);
    if (log_fp) fclose(log_fp);
    return 0;
}