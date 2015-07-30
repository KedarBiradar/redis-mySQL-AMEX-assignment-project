
#define True 1
#define False 0

#define SET 0
#define GET 1
#define EXPIRE 2
#define INVLD_CMD -1


void parse_set_command(char * ,char *,char * );
int check_command_type(char * );
void parse_get_command(char * ,char * );
void parse_expire_command(char * ,char * ,long long * );
/*void start_redis_server();
void start_mysql_server();
void execute_command(char *);*/