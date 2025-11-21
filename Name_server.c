// name_server.c
// Compile: gcc -o name_server name_server.c -lpthread
// Name Server: listens on 9000

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>

#define NM_PORT 9000
#define BUFFER_SIZE 8192
#define MAX_SS 32
#define MAX_FILES 2000
#define MAX_USERS 512
#define MAX_ACCESS 256
#define USERNAME_LEN 64
#define IP_LEN 40
#define FNAME_LEN 256
#define END_MARKER "<END>"
#define LOG_FILE "./nm_log.txt"
#define METADATA_FILE "./nm_metadata.dat"
#define HASH_SIZE 1009 // Prime number for hash table

typedef struct
{
    char ip[IP_LEN];
    int port;
} SSInfo;

typedef struct FileEntry
{
    char name[FNAME_LEN];
    char owner[USERNAME_LEN];
    char ss_ip[IP_LEN];
    int ss_port;
    char read_access[MAX_ACCESS][USERNAME_LEN];
    int read_count;
    char write_access[MAX_ACCESS][USERNAME_LEN];
    int write_count;
    int word_count;
    int char_count;
    int sentence_count;
    char last_access[64];
    struct FileEntry *next; // For hash collision chaining
} FileEntry;

typedef struct
{
    char username[USERNAME_LEN];
    int sock;
} ClientEntry;

typedef struct CacheEntry
{
    char key[FNAME_LEN];
    FileEntry *file;
    time_t timestamp;
    struct CacheEntry *next;
} CacheEntry;

/* Globals */
static SSInfo ss_list[MAX_SS];
static int ss_count = 0;
static pthread_mutex_t ss_mutex = PTHREAD_MUTEX_INITIALIZER;

static FileEntry *file_hash_table[HASH_SIZE]; // Hash table for O(1) lookup
static int file_count = 0;
static pthread_mutex_t files_mutex = PTHREAD_MUTEX_INITIALIZER;

static ClientEntry clients[MAX_USERS];
static int client_count = 0;
static pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

static CacheEntry *cache_head = NULL;
static int cache_size = 0;
#define MAX_CACHE_SIZE 100
static pthread_mutex_t cache_mutex = PTHREAD_MUTEX_INITIALIZER;

static FILE *log_fp = NULL;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Logging */
static void log_message(const char *level, const char *client_ip, int client_port, const char *username, const char *msg)
{
    pthread_mutex_lock(&log_mutex);
    if (!log_fp)
        log_fp = fopen(LOG_FILE, "a");
    if (log_fp)
    {
        time_t now = time(NULL);
        char timebuf[64];
        struct tm tmv;
        localtime_r(&now, &tmv);
        strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", &tmv);
        fprintf(log_fp, "[%s] [%s] [%s:%d] [%s] %s\n", timebuf, level, client_ip, client_port, username, msg);
        fflush(log_fp);
        printf("[%s] [%s] %s\n", timebuf, level, msg);
    }
    pthread_mutex_unlock(&log_mutex);
}

/* Hash function for file names */
static unsigned int hash_string(const char *str)
{
    unsigned int hash = 5381;
    int c;
    while ((c = *str++))
        hash = ((hash << 5) + hash) + c;
    return hash % HASH_SIZE;
}

/* Cache functions */
static FileEntry *cache_get(const char *fname)
{
    pthread_mutex_lock(&cache_mutex);
    CacheEntry *curr = cache_head;
    while (curr)
    {
        if (strcmp(curr->key, fname) == 0)
        {
            curr->timestamp = time(NULL);
            FileEntry *result = curr->file;
            pthread_mutex_unlock(&cache_mutex);
            return result;
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&cache_mutex);
    return NULL;
}

static void cache_put(const char *fname, FileEntry *file)
{
    pthread_mutex_lock(&cache_mutex);

    // Check if already cached
    CacheEntry *curr = cache_head;
    while (curr)
    {
        if (strcmp(curr->key, fname) == 0)
        {
            curr->timestamp = time(NULL);
            curr->file = file;
            pthread_mutex_unlock(&cache_mutex);
            return;
        }
        curr = curr->next;
    }

    // Add new entry
    CacheEntry *new_entry = malloc(sizeof(CacheEntry));
    strncpy(new_entry->key, fname, FNAME_LEN - 1);
    new_entry->file = file;
    new_entry->timestamp = time(NULL);
    new_entry->next = cache_head;
    cache_head = new_entry;
    cache_size++;

    // Evict oldest if cache full
    if (cache_size > MAX_CACHE_SIZE)
    {
        CacheEntry *prev = NULL, *oldest = cache_head;
        time_t oldest_time = cache_head->timestamp;

        curr = cache_head;
        CacheEntry *prev_curr = NULL;
        while (curr)
        {
            if (curr->timestamp < oldest_time)
            {
                oldest_time = curr->timestamp;
                oldest = curr;
                prev = prev_curr;
            }
            prev_curr = curr;
            curr = curr->next;
        }

        if (prev)
            prev->next = oldest->next;
        else
            cache_head = oldest->next;
        free(oldest);
        cache_size--;
    }

    pthread_mutex_unlock(&cache_mutex);
}

static void cache_invalidate(const char *fname)
{
    pthread_mutex_lock(&cache_mutex);
    CacheEntry *curr = cache_head, *prev = NULL;
    while (curr)
    {
        if (strcmp(curr->key, fname) == 0)
        {
            if (prev)
                prev->next = curr->next;
            else
                cache_head = curr->next;
            free(curr);
            cache_size--;
            break;
        }
        prev = curr;
        curr = curr->next;
    }
    pthread_mutex_unlock(&cache_mutex);
}

/* File operations with hash table */
static FileEntry *find_file(const char *fname)
{
    // Check cache first
    FileEntry *cached = cache_get(fname);
    if (cached)
        return cached;

    // Hash table lookup
    pthread_mutex_lock(&files_mutex);
    unsigned int hash = hash_string(fname);
    FileEntry *curr = file_hash_table[hash];

    while (curr)
    {
        if (strcmp(curr->name, fname) == 0)
        {
            pthread_mutex_unlock(&files_mutex);
            cache_put(fname, curr);
            return curr;
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&files_mutex);
    return NULL;
}

static int add_file_metadata(const char *fname, const char *owner, const char *ssip, int ssport)
{
    pthread_mutex_lock(&files_mutex);

    if (file_count >= MAX_FILES)
    {
        pthread_mutex_unlock(&files_mutex);
        return -1;
    }

    FileEntry *new_file = malloc(sizeof(FileEntry));
    memset(new_file, 0, sizeof(FileEntry));

    strncpy(new_file->name, fname, FNAME_LEN - 1);
    strncpy(new_file->owner, owner, USERNAME_LEN - 1);
    strncpy(new_file->ss_ip, ssip, IP_LEN - 1);
    new_file->ss_port = ssport;
    new_file->read_count = 0;
    new_file->write_count = 0;

    // Add owner to both read and write access
    strncpy(new_file->read_access[new_file->read_count++], owner, USERNAME_LEN - 1);
    strncpy(new_file->write_access[new_file->write_count++], owner, USERNAME_LEN - 1);

    new_file->word_count = 0;
    new_file->char_count = 0;
    new_file->sentence_count = 0;
    strcpy(new_file->last_access, "never");

    // Insert into hash table
    unsigned int hash = hash_string(fname);
    new_file->next = file_hash_table[hash];
    file_hash_table[hash] = new_file;

    file_count++;
    pthread_mutex_unlock(&files_mutex);

    cache_put(fname, new_file);
    return 0;
}

static void remove_file_metadata(const char *fname)
{
    pthread_mutex_lock(&files_mutex);

    unsigned int hash = hash_string(fname);
    FileEntry *curr = file_hash_table[hash];
    FileEntry *prev = NULL;

    while (curr)
    {
        if (strcmp(curr->name, fname) == 0)
        {
            if (prev)
                prev->next = curr->next;
            else
                file_hash_table[hash] = curr->next;
            free(curr);
            file_count--;
            break;
        }
        prev = curr;
        curr = curr->next;
    }

    pthread_mutex_unlock(&files_mutex);
    cache_invalidate(fname);
}

static void mark_ss_unavailable(const char *ip, int port)
{
    pthread_mutex_lock(&ss_mutex);
    for (int i = 0; i < ss_count; ++i)
    {
        if (strcmp(ss_list[i].ip, ip) == 0 && ss_list[i].port == port)
        {
            // Remove by swapping with last
            ss_list[i] = ss_list[ss_count - 1];
            ss_count--;
            printf("[NM] Removed unavailable SS %s:%d\n", ip, port);
            break;
        }
    }
    pthread_mutex_unlock(&ss_mutex);
}

static int has_read_permission(FileEntry *file, const char *user)
{
    if (!file)
        return 0;
    if (strcmp(file->owner, user) == 0)
        return 1;
    for (int i = 0; i < file->read_count; ++i)
        if (strcmp(file->read_access[i], user) == 0)
            return 1;
    return 0;
}

static int has_write_permission(FileEntry *file, const char *user)
{
    if (!file)
        return 0;
    if (strcmp(file->owner, user) == 0)
        return 1;
    for (int i = 0; i < file->write_count; ++i)
        if (strcmp(file->write_access[i], user) == 0)
            return 1;
    return 0;
}

/* Persistence functions */
static void save_metadata()
{
    pthread_mutex_lock(&files_mutex);
    FILE *f = fopen(METADATA_FILE, "wb");
    if (!f)
    {
        pthread_mutex_unlock(&files_mutex);
        return;
    }

    fwrite(&file_count, sizeof(int), 1, f);

    for (int i = 0; i < HASH_SIZE; ++i)
    {
        FileEntry *curr = file_hash_table[i];
        while (curr)
        {
            fwrite(curr, sizeof(FileEntry), 1, f);
            curr = curr->next;
        }
    }

    fclose(f);
    pthread_mutex_unlock(&files_mutex);
}

static void load_metadata()
{
    FILE *f = fopen(METADATA_FILE, "rb");
    if (!f)
        return;

    pthread_mutex_lock(&files_mutex);

    int count;
    if (fread(&count, sizeof(int), 1, f) != 1)
    {
        fclose(f);
        pthread_mutex_unlock(&files_mutex);
        return;
    }

    for (int i = 0; i < count; ++i)
    {
        FileEntry *entry = malloc(sizeof(FileEntry));
        if (fread(entry, sizeof(FileEntry), 1, f) != 1)
        {
            free(entry);
            break;
        }

        unsigned int hash = hash_string(entry->name);
        entry->next = file_hash_table[hash];
        file_hash_table[hash] = entry;
        file_count++;
    }

    fclose(f);
    pthread_mutex_unlock(&files_mutex);
    printf("[NM] Loaded %d file metadata entries\n", file_count);
}

/* Utils */
static void send_str_raw(int sock, const char *s)
{
    if (!s)
        return;
    send(sock, s, strlen(s), 0);
}

static void send_str_line(int sock, const char *s)
{
    if (!s)
        return;
    send_str_raw(sock, s);
    send_str_raw(sock, "\n");
}

static ssize_t recv_line(int sock, char *buf, size_t maxlen)
{
    size_t pos = 0;
    while (pos + 1 < maxlen)
    {
        ssize_t r = recv(sock, buf + pos, 1, 0);
        if (r <= 0)
            return r;
        if (buf[pos] == '\n')
        {
            buf[pos] = '\0';
            return (ssize_t)pos;
        }
        pos++;
    }
    buf[pos] = '\0';
    return (ssize_t)pos;
}

/* Forward to SS and relay single-line response */
// static int forward_to_ss_and_relay(const char *ss_ip, int ss_port, const char *cmd, int client_sock) {
//     int s = socket(AF_INET, SOCK_STREAM, 0);
//     if (s < 0) {
//         send_str_line(client_sock, "ERROR: cannot create socket");
//         return 0;
//     }

//     struct sockaddr_in addr = {0};
//     addr.sin_family = AF_INET;
//     addr.sin_port = htons(ss_port);
//     inet_pton(AF_INET, ss_ip, &addr.sin_addr);

//     if (connect(s, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
//         send_str_line(client_sock, "ERROR: cannot connect to storage server");
//         close(s);
//         return 0;
//     }

//     char tmp[BUFFER_SIZE];
//     snprintf(tmp, sizeof(tmp), "%s\n", cmd);
//     send(s, tmp, strlen(tmp), 0);

//     // Read until END_MARKER and relay everything
//     char buf[BUFFER_SIZE];
//     while (1) {
//         ssize_t r = recv_line(s, buf, sizeof(buf));
//         if (r <= 0) break;
//         if (strcmp(buf, END_MARKER) == 0) break;
//         send_str_line(client_sock, buf);
//     }

//     close(s);
//     return 1;
// }

static int forward_to_ss_and_relay(const char *ss_ip, int ss_port, const char *cmd, int client_sock)
{
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0)
    {
        send_str_line(client_sock, "ERROR: cannot create socket");
        return 0;
    }

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(ss_port);
    inet_pton(AF_INET, ss_ip, &addr.sin_addr);

    if (connect(s, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        send_str_line(client_sock, "ERROR: cannot connect to storage server");
        close(s);
        mark_ss_unavailable(ss_ip, ss_port);
        return 0;
    }

    char tmp[BUFFER_SIZE];
    snprintf(tmp, sizeof(tmp), "%s\n", cmd);
    if (send(s, tmp, strlen(tmp), 0) < 0)
    {
        send_str_line(client_sock, "ERROR: storage server disconnected");
        close(s);
        mark_ss_unavailable(ss_ip, ss_port);
        return 0;
    }

    // Read until END_MARKER and relay everything
    char buf[BUFFER_SIZE];
    while (1)
    {
        ssize_t r = recv_line(s, buf, sizeof(buf));
        if (r <= 0)
        {
            mark_ss_unavailable(ss_ip, ss_port);
            break;
        }
        if (strcmp(buf, END_MARKER) == 0)
            break;
        send_str_line(client_sock, buf);
    }

    close(s);
    return 1;
}

/* Forward to SS and get multi-line response */
static char *forward_to_ss_get(const char *ss_ip, int ss_port, const char *cmd)
{
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0)
        return NULL;

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(ss_port);
    inet_pton(AF_INET, ss_ip, &addr.sin_addr);

    if (connect(s, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        close(s);
        return NULL;
    }

    char tmp[BUFFER_SIZE];
    snprintf(tmp, sizeof(tmp), "%s\n", cmd);
    send(s, tmp, strlen(tmp), 0);

    size_t cap = 4096, len = 0;
    char *out = malloc(cap);
    if (!out)
    {
        close(s);
        return NULL;
    }
    out[0] = '\0';

    char line[BUFFER_SIZE];
    while (1)
    {
        ssize_t r = recv_line(s, line, sizeof(line));
        if (r <= 0)
            break;
        if (strcmp(line, END_MARKER) == 0)
            break;

        size_t need = strlen(line) + 2;
        if (len + need + 1 > cap)
        {
            cap = (cap + need) * 2;
            char *nb = realloc(out, cap);
            if (!nb)
            {
                free(out);
                close(s);
                return NULL;
            }
            out = nb;
        }
        strcat(out, line);
        strcat(out, "\n");
        len = strlen(out);
    }

    close(s);
    return out;
}

/* Handle SS registration */
static void handle_ss_registration(const char *line, const char *client_ip)
{
    char ip[IP_LEN];
    int port = 0, n = 0;
    const char *p = line + strlen("REGISTER_SS");
    while (*p == ' ')
        p++;

    int consumed = 0;
    int got = sscanf(p, "%39s %d %d%n", ip, &port, &n, &consumed);
    if (got < 3)
        return;
    p += consumed;

    pthread_mutex_lock(&ss_mutex);
    if (ss_count < MAX_SS)
    {
        strncpy(ss_list[ss_count].ip, ip, IP_LEN - 1);
        ss_list[ss_count].port = port;
        ss_count++;
    }
    pthread_mutex_unlock(&ss_mutex);

    char logmsg[512];
    snprintf(logmsg, sizeof(logmsg), "Registered SS %s:%d with %d files", ip, port, n);
    log_message("INFO", client_ip, port, "SYSTEM", logmsg);

    for (int i = 0; i < n; ++i)
    {
        char fname[FNAME_LEN];
        while (*p == ' ')
            p++;
        if (sscanf(p, "%255s%n", fname, &consumed) != 1)
            break;
        p += consumed;

        if (!find_file(fname))
        {
            add_file_metadata(fname, "unknown", ip, port);
        }
    }

    save_metadata();
}

/* Connection handler */
static void *connection_handler(void *arg)
{
    int sock = *((int *)arg);
    free(arg);

    struct sockaddr_in peer;
    socklen_t plen = sizeof(peer);
    getpeername(sock, (struct sockaddr *)&peer, &plen);
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &peer.sin_addr, client_ip, sizeof(client_ip));
    int client_port = ntohs(peer.sin_port);

    char buf[BUFFER_SIZE];
    ssize_t r = recv_line(sock, buf, sizeof(buf));
    if (r <= 0)
    {
        close(sock);
        return NULL;
    }

    if (strncmp(buf, "REGISTER_SS", 11) == 0)
    {
        handle_ss_registration(buf, client_ip);
        close(sock);
        return NULL;
    }

    // Client connection - username
    char username[USERNAME_LEN];
    strncpy(username, buf, USERNAME_LEN - 1);
    username[USERNAME_LEN - 1] = '\0';

    char logmsg[512];
    snprintf(logmsg, sizeof(logmsg), "Client connected: %s", username);
    log_message("INFO", client_ip, client_port, username, logmsg);

    pthread_mutex_lock(&clients_mutex);
    if (client_count < MAX_USERS)
    {
        strncpy(clients[client_count].username, username, USERNAME_LEN - 1);
        clients[client_count].sock = sock;
        client_count++;
    }
    else
    {
        send_str_line(sock, "ERROR: server busy");
        close(sock);
        pthread_mutex_unlock(&clients_mutex);
        return NULL;
    }
    pthread_mutex_unlock(&clients_mutex);

    while (1)
    {
        memset(buf, 0, sizeof(buf));
        int n = recv_line(sock, buf, sizeof(buf));
        if (n <= 0)
            break;

        snprintf(logmsg, sizeof(logmsg), "REQUEST: %s", buf);
        log_message("INFO", client_ip, client_port, username, logmsg);

        if (strncmp(buf, "VIEW", 4) == 0)
        {
            int show_all = (strstr(buf, "-a") != NULL || strstr(buf, "VIEW -al") != NULL || strstr(buf, "VIEW -la") != NULL);
            int show_detail = (strstr(buf, "-l") != NULL || strstr(buf, "VIEW -al") != NULL || strstr(buf, "VIEW -la") != NULL);

            char out[BUFFER_SIZE];
            out[0] = '\0';
            strcat(out, "Files:\n");

            pthread_mutex_lock(&files_mutex);
            for (int i = 0; i < HASH_SIZE; ++i)
            {
                FileEntry *curr = file_hash_table[i];
                while (curr)
                {
                    if (show_all || strcmp(curr->owner, username) == 0)
                    {
                        strcat(out, curr->name);
                        if (show_detail)
                        {
                            char tmp[512];
                            snprintf(tmp, sizeof(tmp), " (owner:%s ss:%s:%d words:%d chars:%d sentences:%d last:%s)",
                                     curr->owner, curr->ss_ip, curr->ss_port,
                                     curr->word_count, curr->char_count, curr->sentence_count, curr->last_access);
                            strcat(out, tmp);
                        }
                        strcat(out, "\n");
                    }
                    curr = curr->next;
                }
            }
            pthread_mutex_unlock(&files_mutex);

            send_str_raw(sock, out);
            send_str_line(sock, END_MARKER);
            log_message("INFO", client_ip, client_port, username, "ACK: VIEW complete");
        }
        else if (strncmp(buf, "LIST", 4) == 0)
        {
            char out[BUFFER_SIZE];
            out[0] = '\0';
            strcat(out, "Users:\n");

            pthread_mutex_lock(&clients_mutex);
            for (int i = 0; i < client_count; ++i)
            {
                strcat(out, clients[i].username);
                strcat(out, "\n");
            }
            pthread_mutex_unlock(&clients_mutex);

            send_str_raw(sock, out);
            send_str_line(sock, END_MARKER);
            log_message("INFO", client_ip, client_port, username, "ACK: LIST complete");
        }
        else if (strncmp(buf, "CREATE ", 7) == 0)
        {
            char fname[FNAME_LEN];
            if (sscanf(buf + 7, "%255s", fname) != 1)
            {
                send_str_line(sock, "ERROR: CREATE requires filename");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "CREATE missing filename");
                continue;
            }

            FileEntry *existing = find_file(fname);
            if (existing)
            {
                send_str_line(sock, "ERROR: File already exists");
                send_str_line(sock, END_MARKER);
                snprintf(logmsg, sizeof(logmsg), "CREATE failed: %s exists", fname);
                log_message("ERROR", client_ip, client_port, username, logmsg);
                continue;
            }

            pthread_mutex_lock(&ss_mutex);
            if (ss_count == 0)
            {
                pthread_mutex_unlock(&ss_mutex);
                send_str_line(sock, "ERROR: No storage server available");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "CREATE failed: no SS");
                continue;
            }

            static int rr = 0;
            int ssid = rr % ss_count;
            rr++;
            char ssip[IP_LEN];
            int ssport = ss_list[ssid].port;
            strcpy(ssip, ss_list[ssid].ip);
            pthread_mutex_unlock(&ss_mutex);

            char cmd[BUFFER_SIZE];
            snprintf(cmd, sizeof(cmd), "CREATE %s %s", fname, username);

            if (!forward_to_ss_and_relay(ssip, ssport, cmd, sock))
            {
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "CREATE failed: SS unreachable");
                continue;
            }

            add_file_metadata(fname, username, ssip, ssport);
            save_metadata();

            send_str_line(sock, END_MARKER);
            snprintf(logmsg, sizeof(logmsg), "ACK: Created %s", fname);
            log_message("INFO", client_ip, client_port, username, logmsg);
        }
        else if (strncmp(buf, "DELETE ", 7) == 0)
        {
            char fname[FNAME_LEN];
            if (sscanf(buf + 7, "%255s", fname) != 1)
            {
                send_str_line(sock, "ERROR: DELETE requires filename");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "DELETE missing filename");
                continue;
            }

            FileEntry *file = find_file(fname);
            if (!file)
            {
                send_str_line(sock, "ERROR: File not found");
                send_str_line(sock, END_MARKER);
                snprintf(logmsg, sizeof(logmsg), "DELETE failed: %s not found", fname);
                log_message("ERROR", client_ip, client_port, username, logmsg);
                continue;
            }

            if (strcmp(file->owner, username) != 0)
            {
                send_str_line(sock, "ERROR: Only owner can delete");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "DELETE failed: not owner");
                continue;
            }

            char cmd[BUFFER_SIZE];
            snprintf(cmd, sizeof(cmd), "DELETE %s", fname);

            if (forward_to_ss_and_relay(file->ss_ip, file->ss_port, cmd, sock))
            {
                remove_file_metadata(fname);
                save_metadata();
                send_str_line(sock, END_MARKER);
                snprintf(logmsg, sizeof(logmsg), "ACK: Deleted %s", fname);
                log_message("INFO", client_ip, client_port, username, logmsg);
            }
            else
            {
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "DELETE failed: SS error");
            }
        }
        else if (strncmp(buf, "LOOKUP ", 7) == 0)
        {
            char fname[FNAME_LEN], op[32];
            if (sscanf(buf + 7, "%255s %31s", fname, op) < 1)
            {
                send_str_line(sock, "ERROR: LOOKUP requires filename");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "LOOKUP missing filename");
                continue;
            }

            FileEntry *file = find_file(fname);
            if (!file)
            {
                send_str_line(sock, "ERROR: File not found");
                send_str_line(sock, END_MARKER);
                snprintf(logmsg, sizeof(logmsg), "LOOKUP failed: %s not found", fname);
                log_message("ERROR", client_ip, client_port, username, logmsg);
                continue;
            }

            if ((strcmp(op, "READ") == 0 || strcmp(op, "STREAM") == 0) && !has_read_permission(file, username))
            {
                send_str_line(sock, "ERROR: Read access denied");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "LOOKUP denied: no read access");
                continue;
            }

            if ((strcmp(op, "WRITE") == 0) && !has_write_permission(file, username))
            {
                send_str_line(sock, "ERROR: Write access denied");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "LOOKUP denied: no write access");
                continue;
            }

            char resp[256];
            snprintf(resp, sizeof(resp), "SSINFO %s %d", file->ss_ip, file->ss_port);
            send_str_line(sock, resp);
            send_str_line(sock, END_MARKER);
            snprintf(logmsg, sizeof(logmsg), "ACK: LOOKUP %s -> %s:%d", fname, file->ss_ip, file->ss_port);
            log_message("INFO", client_ip, client_port, username, logmsg);
        }
        else if (strncmp(buf, "INFO ", 5) == 0)
        {
            char fname[FNAME_LEN];
            if (sscanf(buf + 5, "%255s", fname) != 1)
            {
                send_str_line(sock, "ERROR: INFO requires filename");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "INFO missing filename");
                continue;
            }

            FileEntry *file = find_file(fname);
            if (!file)
            {
                send_str_line(sock, "ERROR: File not found");
                send_str_line(sock, END_MARKER);
                snprintf(logmsg, sizeof(logmsg), "INFO failed: %s not found", fname);
                log_message("ERROR", client_ip, client_port, username, logmsg);
                continue;
            }

            char cmd[BUFFER_SIZE];
            snprintf(cmd, sizeof(cmd), "INFO %s", fname);
            char *resp = forward_to_ss_get(file->ss_ip, file->ss_port, cmd);

            if (!resp)
                send_str_line(sock, "ERROR: Storage server unreachable");
            else
            {
                send_str_raw(sock, resp);
                free(resp);
            }
            send_str_line(sock, END_MARKER);
            snprintf(logmsg, sizeof(logmsg), "ACK: INFO for %s", fname);
            log_message("INFO", client_ip, client_port, username, logmsg);
        }
        else if (strncmp(buf, "ADDACCESS ", 10) == 0)
        {
            char flag[8], fname[FNAME_LEN], target[USERNAME_LEN];
            if (sscanf(buf + 10, "%7s %255s %63s", flag, fname, target) < 3)
            {
                send_str_line(sock, "ERROR: invalid ADDACCESS");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "ADDACCESS invalid format");
                continue;
            }

            FileEntry *file = find_file(fname);
            if (!file)
            {
                send_str_line(sock, "ERROR: File not found");
                send_str_line(sock, END_MARKER);
                snprintf(logmsg, sizeof(logmsg), "ADDACCESS failed: %s not found", fname);
                log_message("ERROR", client_ip, client_port, username, logmsg);
                continue;
            }

            if (strcmp(file->owner, username) != 0)
            {
                send_str_line(sock, "ERROR: Only owner can add access");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "ADDACCESS failed: not owner");
                continue;
            }

            if (strcmp(flag, "-R") == 0)
            {
                int found = 0;
                for (int i = 0; i < file->read_count; ++i)
                    if (strcmp(file->read_access[i], target) == 0)
                        found = 1;
                if (!found && file->read_count < MAX_ACCESS)
                    strncpy(file->read_access[file->read_count++], target, USERNAME_LEN - 1);
                send_str_line(sock, "ACK: Read access added");
                snprintf(logmsg, sizeof(logmsg), "ACK: Read access for %s to %s", target, fname);
                log_message("INFO", client_ip, client_port, username, logmsg);
            }
            // else if (strcmp(flag, "-W") == 0)
            // {
            //     int found = 0;
            //     for (int i = 0; i < file->write_count; ++i)
            //         if (strcmp(file->write_access[i], target) == 0)
            //             found = 1;
            //     if (!found && file->write_count < MAX_ACCESS)
            //         strncpy(file->write_access[file->write_count++], target, USERNAME_LEN - 1);
            //     send_str_line(sock, "ACK: Write access added");
            //     snprintf(logmsg, sizeof(logmsg), "ACK: Write access for %s to %s", target, fname);
            //     log_message("INFO", client_ip, client_port, username, logmsg);
            // }
            else if (strcmp(flag, "-W") == 0) {
                // Add write access
                int found = 0;
                for (int i = 0; i < file->write_count; ++i)
                    if (strcmp(file->write_access[i], target) == 0)
                        found = 1;
                if (!found && file->write_count < MAX_ACCESS)
                    strncpy(file->write_access[file->write_count++], target, USERNAME_LEN - 1);
                
                // Also add read access (per spec: -W grants both)
                found = 0;
                for (int i = 0; i < file->read_count; ++i)
                    if (strcmp(file->read_access[i], target) == 0)
                        found = 1;
                if (!found && file->read_count < MAX_ACCESS)
                    strncpy(file->read_access[file->read_count++], target, USERNAME_LEN - 1);
                
                send_str_line(sock, "ACK: Write and read access added");
                snprintf(logmsg, sizeof(logmsg), "ACK: Write+read access for %s to %s", target, fname);
                log_message("INFO", client_ip, client_port, username, logmsg);
            }
            
            else
            {
                send_str_line(sock, "ERROR: unknown flag");
                log_message("ERROR", client_ip, client_port, username, "ADDACCESS unknown flag");
            }
            send_str_line(sock, END_MARKER);
            save_metadata();
        }
        else if (strncmp(buf, "REMACCESS ", 10) == 0)
        {
            char fname[FNAME_LEN], target[USERNAME_LEN];
            if (sscanf(buf + 10, "%255s %63s", fname, target) < 2)
            {
                send_str_line(sock, "ERROR: invalid REMACCESS");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "REMACCESS invalid format");
                continue;
            }

            FileEntry *file = find_file(fname);
            if (!file)
            {
                send_str_line(sock, "ERROR: File not found");
                send_str_line(sock, END_MARKER);
                snprintf(logmsg, sizeof(logmsg), "REMACCESS failed: %s not found", fname);
                log_message("ERROR", client_ip, client_port, username, logmsg);
                continue;
            }

            if (strcmp(file->owner, username) != 0)
            {
                send_str_line(sock, "ERROR: Only owner can remove access");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "REMACCESS failed: not owner");
                continue;
            }

            for (int i = 0; i < file->read_count; ++i)
                if (strcmp(file->read_access[i], target) == 0)
                {
                    file->read_count--;
                    if (i < file->read_count)
                        strcpy(file->read_access[i], file->read_access[file->read_count]);
                    break;
                }

            for (int i = 0; i < file->write_count; ++i)
                if (strcmp(file->write_access[i], target) == 0)
                {
                    file->write_count--;
                    if (i < file->write_count)
                        strcpy(file->write_access[i], file->write_access[file->write_count]);
                    break;
                }

            send_str_line(sock, "ACK: access removed");
            send_str_line(sock, END_MARKER);
            snprintf(logmsg, sizeof(logmsg), "ACK: Removed access for %s from %s", target, fname);
            log_message("INFO", client_ip, client_port, username, logmsg);
            save_metadata();
        }
        else if (strncmp(buf, "UNDO ", 5) == 0)
        {
            char fname[FNAME_LEN];
            if (sscanf(buf + 5, "%255s", fname) != 1)
            {
                send_str_line(sock, "ERROR: UNDO requires filename");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "UNDO missing filename");
                continue;
            }

            FileEntry *file = find_file(fname);
            if (!file)
            {
                send_str_line(sock, "ERROR: File not found");
                send_str_line(sock, END_MARKER);
                snprintf(logmsg, sizeof(logmsg), "UNDO failed: %s not found", fname);
                log_message("ERROR", client_ip, client_port, username, logmsg);
                continue;
            }

            char cmd[BUFFER_SIZE];
            snprintf(cmd, sizeof(cmd), "UNDO %s", fname);
            forward_to_ss_and_relay(file->ss_ip, file->ss_port, cmd, sock);
            send_str_line(sock, END_MARKER);
            snprintf(logmsg, sizeof(logmsg), "UNDO on %s", fname);
            log_message("INFO", client_ip, client_port, username, logmsg);
        }
        else if (strncmp(buf, "EXEC ", 5) == 0)
        {
            char fname[FNAME_LEN];
            if (sscanf(buf + 5, "%255s", fname) != 1)
            {
                send_str_line(sock, "ERROR: EXEC requires filename");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "EXEC missing filename");
                continue;
            }

            FileEntry *file = find_file(fname);
            if (!file)
            {
                send_str_line(sock, "ERROR: File not found");
                send_str_line(sock, END_MARKER);
                snprintf(logmsg, sizeof(logmsg), "EXEC failed: %s not found", fname);
                log_message("ERROR", client_ip, client_port, username, logmsg);
                continue;
            }

            if (!has_read_permission(file, username))
            {
                send_str_line(sock, "ERROR: Read access denied");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "EXEC denied: no read access");
                continue;
            }

            char cmd[BUFFER_SIZE];
            snprintf(cmd, sizeof(cmd), "READ %s", fname);
            char *filecontent = forward_to_ss_get(file->ss_ip, file->ss_port, cmd);

            if (!filecontent)
            {
                send_str_line(sock, "ERROR: Storage server unreachable");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "EXEC failed: SS unreachable");
                continue;
            }

            char tmpname[256];
            snprintf(tmpname, sizeof(tmpname), "/tmp/nm_exec_%d_%ld.sh", (int)getpid(), (long)time(NULL));
            FILE *tf = fopen(tmpname, "w");
            if (!tf)
            {
                free(filecontent);
                send_str_line(sock, "ERROR: cannot create temp script");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "EXEC failed: temp file error");
                continue;
            }

            fputs(filecontent, tf);
            fclose(tf);
            free(filecontent);

            char popencmd[512];
            snprintf(popencmd, sizeof(popencmd), "/bin/sh %s 2>&1", tmpname);
            FILE *fp = popen(popencmd, "r");
            if (!fp)
            {
                unlink(tmpname);
                send_str_line(sock, "ERROR: execution failed");
                send_str_line(sock, END_MARKER);
                log_message("ERROR", client_ip, client_port, username, "EXEC failed: popen error");
                continue;
            }

            char outbuf[1024];
            while (fgets(outbuf, sizeof(outbuf), fp))
                send_str_raw(sock, outbuf);

            pclose(fp);
            unlink(tmpname);
            send_str_line(sock, END_MARKER);
            snprintf(logmsg, sizeof(logmsg), "ACK: EXEC on %s", fname);
            log_message("INFO", client_ip, client_port, username, logmsg);
        }
        else
        {
            send_str_line(sock, "ERROR: Unknown command");
            send_str_line(sock, END_MARKER);
            snprintf(logmsg, sizeof(logmsg), "ERROR: Unknown command: %s", buf);
            log_message("ERROR", client_ip, client_port, username, logmsg);
        }
    }

    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < client_count; ++i)
    {
        if (clients[i].sock == sock)
        {
            clients[i] = clients[client_count - 1];
            client_count--;
            break;
        }
    }
    pthread_mutex_unlock(&clients_mutex);

    snprintf(logmsg, sizeof(logmsg), "Client disconnected: %s", username);
    log_message("INFO", client_ip, client_port, username, logmsg);
    close(sock);
    return NULL;
}

int main(void)
{
    memset(file_hash_table, 0, sizeof(file_hash_table));

    load_metadata();

    int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock < 0)
    {
        perror("socket");
        exit(1);
    }

    int opt = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in serv = {0};
    serv.sin_family = AF_INET;
    serv.sin_addr.s_addr = INADDR_ANY;
    serv.sin_port = htons(NM_PORT);

    if (bind(listen_sock, (struct sockaddr *)&serv, sizeof(serv)) < 0)
    {
        perror("bind");
        exit(1);
    }

    if (listen(listen_sock, 50) < 0)
    {
        perror("listen");
        exit(1);
    }

    printf("[NM] Listening on %d\n", NM_PORT);
    log_message("INFO", "0.0.0.0", NM_PORT, "SYSTEM", "Name Server started");

    while (1)
    {
        int *c = malloc(sizeof(int));
        struct sockaddr_in cli;
        socklen_t alen = sizeof(cli);
        *c = accept(listen_sock, (struct sockaddr *)&cli, &alen);
        if (*c < 0)
        {
            free(c);
            continue;
        }
        pthread_t tid;
        pthread_create(&tid, NULL, connection_handler, c);
        pthread_detach(tid);
    }

    close(listen_sock);
    if (log_fp)
        fclose(log_fp);
    return 0;
}