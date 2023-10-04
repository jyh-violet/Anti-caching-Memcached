#ifndef PROTO_TEXT_H
#define PROTO_TEXT_H

/* text protocol handlers */
void complete_nread_ascii(conn *c);
int try_read_command_asciiauth(conn *c);
int try_read_command_ascii(conn *c);
void process_command_ascii(conn *c, char *command);
typedef struct token_s {
    char *value;
    size_t length;
} token_t;
void process_get_command_mem_only(conn *c, token_t *tokens, size_t ntokens, bool return_cas, bool should_touch);
void process_update_command(conn *c, token_t *tokens, const size_t ntokens, int comm, bool handle_cas);
void process_get_command(conn *c, token_t *tokens, size_t ntokens, bool return_cas, bool should_touch);
bool process_update_command_sep(void *storage, char* key, char* value, int key_n, int value_n, bool load);
#endif
