// client.c
// Compile: gcc -o client client.c -lpthread
// Client connects to NM and then to SS for READ/WRITE/STREAM

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define NM_IP "10.14.66.6"
#define NM_PORT 9000
#define BUFFER_SIZE 8192
#define USERNAME_LEN 64
#define FNAME_LEN 256
#define END_MARKER "<END>"

static char username[USERNAME_LEN];

static int connect_to_host(const char *ip, int port) {
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) return -1;
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &addr.sin_addr);
    
    if (connect(s, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(s);
        return -1;
    }
    return s;
}

static int send_all(int sock, const char *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t s = send(sock, buf + sent, len - sent, 0);
        if (s <= 0) return -1;
        sent += (size_t)s;
    }
    return 0;
}

static int send_line(int sock, const char *line) {
    char tmp[BUFFER_SIZE];
    int n = snprintf(tmp, sizeof(tmp), "%s\n", line);
    if (n < 0 || n >= (int)sizeof(tmp)) return -1;
    return send_all(sock, tmp, (size_t)n);
}

static ssize_t recv_line(int sock, char *buf, size_t maxlen) {
    size_t pos = 0;
    while (pos + 1 < maxlen) {
        ssize_t r = recv(sock, buf + pos, 1, 0);
        if (r <= 0) return r;
        if (buf[pos] == '\n') {
            buf[pos] = '\0';
            return (ssize_t)pos;
        }
        pos++;
    }
    buf[pos] = '\0';
    return (ssize_t)pos;
}

static char *recv_until_end(int sock) {
    size_t cap = 4096, len = 0;
    char *out = malloc(cap);
    if (!out) return NULL;
    out[0] = '\0';
    
    char line[4096];
    while (1) {
        ssize_t r = recv_line(sock, line, sizeof(line));
        if (r <= 0) {
            if (len == 0) {
                free(out);
                return NULL;
            }
            break;
        }
        
        if (strcmp(line, END_MARKER) == 0) break;
        
        size_t need = strlen(line) + 2;
        if (len + need + 1 > cap) {
            cap = (cap + need) * 2;
            char *nb = realloc(out, cap);
            if (!nb) {
                free(out);
                return NULL;
            }
            out = nb;
        }
        strcat(out, line);
        strcat(out, "\n");
        len = strlen(out);
    }
    return out;
}

int main(void) {
    printf("Enter username: ");
    if (!fgets(username, sizeof(username), stdin)) return 1;
    username[strcspn(username, "\r\n")] = '\0';
    
    if (strlen(username) == 0) {
        printf("Username required\n");
        return 1;
    }
    
    int nm = connect_to_host(NM_IP, NM_PORT);
    if (nm < 0) {
        perror("connect to NM");
        return 1;
    }
    
    if (send_line(nm, username) != 0) {
        perror("send username");
        close(nm);
        return 1;
    }
    
    printf("Connected to NM as '%s'\n", username);
    printf("Commands: VIEW, READ, WRITE, STREAM, CREATE, DELETE, LIST, INFO, ADDACCESS, REMACCESS, UNDO, EXEC, EXIT\n");
    printf("Examples:\n");
    printf("  VIEW -al\n");
    printf("  CREATE myfile.txt\n");
    printf("  WRITE myfile.txt 0\n");
    printf("  READ myfile.txt\n");
    printf("  STREAM myfile.txt\n");
    printf("  ADDACCESS -R myfile.txt alice\n");
    printf("  INFO myfile.txt\n");
    printf("  EXEC script.sh\n\n");
    
    char line[BUFFER_SIZE];
    while (1) {
        printf("> ");
        if (!fgets(line, sizeof(line), stdin)) break;
        line[strcspn(line, "\r\n")] = '\0';
        if (strlen(line) == 0) continue;
        
        if (strncmp(line, "EXIT", 4) == 0) {
            printf("Exiting...\n");
            break;
        }
        
        // READ, WRITE, STREAM require LOOKUP first
        if (strncmp(line, "READ ", 5) == 0 || strncmp(line, "WRITE ", 6) == 0 || strncmp(line, "STREAM ", 7) == 0) {
            char cmd[512];
            char op[16], fname[FNAME_LEN];
            int sidx = 0;
            
            if (sscanf(line, "%15s %255s %d", op, fname, &sidx) < 2) {
                printf("Usage: %s <filename> [sentence_index]\n", op);
                continue;
            }
            
            // Send LOOKUP to NM
            snprintf(cmd, sizeof(cmd), "LOOKUP %s %s", fname,
                     (strncmp(op, "WRITE", 5) == 0) ? "WRITE" :
                     (strncmp(op, "STREAM", 6) == 0) ? "STREAM" : "READ");
            
            if (send_line(nm, cmd) != 0) {
                printf("Failed to send to NM\n");
                break;
            }
            
            // Receive SSINFO or ERROR
            char resp[BUFFER_SIZE];
            ssize_t r = recv_line(nm, resp, sizeof(resp));
            if (r <= 0) {
                printf("NM disconnected\n");
                break;
            }
            
            // Check for ERROR response
            if (strncmp(resp, "ERROR:", 6) == 0) {
                printf("%s\n", resp);
                // Consume END_MARKER
                char *out = recv_until_end(nm);
                if (out) free(out);
                continue;
            }
            
            // Parse SSINFO
            if (strncmp(resp, "SSINFO", 6) == 0) {
                char ssip[64];
                int ssport = 0;
                if (sscanf(resp + 7, "%63s %d", ssip, &ssport) != 2) {
                    printf("Invalid SSINFO response\n");
                    // Consume END_MARKER
                    char *out = recv_until_end(nm);
                    if (out) free(out);
                    continue;
                }
                
                // Consume END_MARKER from LOOKUP response
                char end_line[256];
                recv_line(nm, end_line, sizeof(end_line));
                
                // Connect to Storage Server
                int ss = connect_to_host(ssip, ssport);
                if (ss < 0) {
                    printf("Cannot connect to Storage Server %s:%d\n", ssip, ssport);
                    continue;
                }
                
                if (strncmp(op, "READ", 4) == 0) {
                    char sendb[512];
                    snprintf(sendb, sizeof(sendb), "READ %s", fname);
                    if (send_line(ss, sendb) != 0) {
                        close(ss);
                        printf("Failed to send READ to SS\n");
                        continue;
                    }
                    
                    char *out = recv_until_end(ss);
                    if (out) {
                        printf("%s", out);
                        free(out);
                    } else {
                        printf("No response from Storage Server\n");
                    }
                    close(ss);
                }
                else if (strncmp(op, "STREAM", 6) == 0) {
                    char sendb[512];
                    snprintf(sendb, sizeof(sendb), "STREAM %s", fname);
                    if (send_line(ss, sendb) != 0) {
                        close(ss);
                        printf("Failed to send STREAM to SS\n");
                        continue;
                    }
                    
                    printf("\n[Streaming...]\n");
                    char buf[4096];
                    int stop_found = 0;
                    while (!stop_found) {
                        ssize_t n = recv(ss, buf, sizeof(buf) - 1, 0);
                        if (n <= 0) break;
                        buf[n] = '\0';
                        
                        char *stop_pos = strstr(buf, "STOP");
                        if (stop_pos) {
                            *stop_pos = '\0';
                            printf("%s", buf);
                            stop_found = 1;
                            break;
                        } else {
                            printf("%s", buf);
                        }
                    }
                    
                    // Consume remaining data until END_MARKER
                    char line2[256];
                    while (1) {
                        ssize_t rr = recv_line(ss, line2, sizeof(line2));
                        if (rr <= 0) break;
                        if (strcmp(line2, END_MARKER) == 0) break;
                    }
                    
                    printf("\n[Stream complete]\n");
                    close(ss);
                }
                else if (strncmp(op, "WRITE", 5) == 0) {
                    char sendb[512];
                    snprintf(sendb, sizeof(sendb), "WRITE %s %d", fname, sidx);
                    if (send_line(ss, sendb) != 0) {
                        close(ss);
                        printf("Failed to send WRITE to SS\n");
                        continue;
                    }
                    
                    printf("Enter sentence content (end with ETIRW on its own line):\n");
                    while (1) {
                        char l[1024];
                        if (!fgets(l, sizeof(l), stdin)) break;
                        l[strcspn(l, "\r\n")] = '\0';
                        
                        if (send_all(ss, l, strlen(l)) != 0) {
                            printf("Failed to send to SS\n");
                            break;
                        }
                        if (send_all(ss, "\n", 1) != 0) {
                            printf("Failed to send newline to SS\n");
                            break;
                        }
                        
                        if (strcmp(l, "ETIRW") == 0) break;
                    }
                    
                    char *out = recv_until_end(ss);
                    if (out) {
                        printf("%s", out);
                        free(out);
                    } else {
                        printf("No response from Storage Server\n");
                    }
                    close(ss);
                }
            } else {
                printf("Unexpected response: %s\n", resp);
                // Consume END_MARKER
                char *out = recv_until_end(nm);
                if (out) free(out);
            }
        }
        // All other commands go through NM directly
        else if (strncmp(line, "CREATE ", 7) == 0 || 
                 strncmp(line, "DELETE ", 7) == 0 || 
                 strncmp(line, "VIEW", 4) == 0 || 
                 strncmp(line, "LIST", 4) == 0 || 
                 strncmp(line, "INFO ", 5) == 0 || 
                 strncmp(line, "ADDACCESS ", 10) == 0 || 
                 strncmp(line, "REMACCESS ", 10) == 0 || 
                 strncmp(line, "UNDO ", 5) == 0 || 
                 strncmp(line, "EXEC ", 5) == 0) {
            
            if (send_line(nm, line) != 0) {
                printf("Failed to send to NM\n");
                break;
            }
            
            char *out = recv_until_end(nm);
            if (out) {
                printf("%s", out);
                free(out);
            } else {
                printf("No response from NM\n");
                break;
            }
        }
        else {
            printf("Unknown command. Available commands:\n");
            printf("  VIEW [-a] [-l]\n");
            printf("  LIST\n");
            printf("  CREATE <filename>\n");
            printf("  READ <filename>\n");
            printf("  WRITE <filename> <sentence_index>\n");
            printf("  STREAM <filename>\n");
            printf("  DELETE <filename>\n");
            printf("  INFO <filename>\n");
            printf("  ADDACCESS -R|-W <filename> <username>\n");
            printf("  REMACCESS <filename> <username>\n");
            printf("  UNDO <filename>\n");
            printf("  EXEC <filename>\n");
            printf("  EXIT\n");
        }
    }
    
    close(nm);
    return 0;
}