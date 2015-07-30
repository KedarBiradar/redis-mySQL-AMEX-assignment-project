#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <my_global.h>
#include <mysql.h>
#include "hiredis.h"
#include "customProxy.h"

typedef int Bool;

Bool is_redis_up = False;
Bool is_mysql_up = False;

char redis_server_ip[32]="127.0.0.1";
int redis_server_port=6379;

char sql_server_ip[32] = "127.0.0.1";
char sql_server_user_id[128] = "root";
char sql_server_user_pass[128] = "12345";
char sql_server_db_name[128] = "redis_backup";

const int redis_check_freq_sec = 5;
const int mysql_check_freq_sec = 8;
const int key_size = 128;
const int value_size = 2048;

redisContext *context;
MYSQL *mySQLcon;

// function to connect redis server with ip and port
void connect_to_redis(){
	context = redisConnect(redis_server_ip,redis_server_port);
	if(context->err){
		is_redis_up = False;
	}
	else{
		is_redis_up = True;	
	}
}

// periodic service to check redis server state
void * check_redis_server_state()
{
	connect_to_redis();
	while(True){
		if(is_redis_up == False ){

			redisReply *reply;
			reply = redisCommand(context,"PING");

			if(reply == NULL || reply->type == REDIS_REPLY_ERROR ){
				is_redis_up = False;
				connect_to_redis();
			}
			else{
				is_redis_up = True;
			}
			freeReplyObject(reply);
		}	
		sleep(redis_check_freq_sec);
	}
}

void connect_to_mysql(){
	
	if(mysql_real_connect(mySQLcon , sql_server_ip , sql_server_user_id , sql_server_user_pass , sql_server_db_name , 0 , NULL , 0) == NULL){
		is_mysql_up = False;
	}
	else{
		is_mysql_up = True;
	}
}

void * check_mysql_server_state(){
	mySQLcon = mysql_init( NULL );
	connect_to_mysql();

	while(True){
		//check connection with mysql and reconnect if needed
		if( mysql_ping(mySQLcon) == 0 ){
			is_mysql_up = True;
		}
		else{
			is_mysql_up = False;
			connect_to_mysql();
		}
		sleep(mysql_check_freq_sec);
	}
}
/* redis is down serve get query from mysql*/
void getCommand_from_mysql(char *command, char * value){

	char key[key_size];
	
	if(is_mysql_up){
		parse_get_command(command,key);
		//connect to mysql database and fetch result from there
		char statement[512];
		snprintf(statement, sizeof(statement) , "select value,ttl from redisStore where keyID = ('%s');" , key);

		if(mysql_query(mySQLcon, statement) == 0 ){					//successful execution

			MYSQL_RES *result = mysql_store_result(mySQLcon);
	  
			if (result == NULL) {
	  			strcpy(value,"(nil)");
	  			return;
			}
	  		int num_rows = mysql_num_rows(result);
	  		if(num_rows == 0 || num_rows > 1){
	  			strcpy(value,"(nil)");
	  		}
	  		else{
		  		MYSQL_ROW row;
		  		while ((row = mysql_fetch_row(result))){
		  			long long ttl = atoll(row[1]);

		  			// check fore ttl value. If expires is mot mentioned then default ttl value is -1.
		  			if(ttl == -1 || ttl >= time(NULL)){
		      			strcpy(value,row[0]);
		  			}
		      		else{
		      			strcpy(value,"(nil)");
		      			snprintf(statement, sizeof(statement) , "delete from redisStore where keyID = ('%s');" , key);
						mysql_query(mySQLcon, statement);
		      		}
		      		break;
				}
			}	
		  			mysql_free_result(result);
		}
		else{
			is_mysql_up = False;
			strcpy(value,"(nil)");
		}
	}
	else{
		strcpy(value,"(nil)");
	}
}

/* Main function to execute GET command */
void getCommand(char *command ,char * value){

	redisReply *reply;
	reply = redisCommand(context, command);

	if( reply == NULL ){
		// redis down serve from mySQL
		is_redis_up = False;
		printf("From mySQL\n");
		getCommand_from_mysql(command,value);
	}

	else{
		//redis up and functioning
		printf("From Redis\n");
		if( reply->type == REDIS_REPLY_NIL ){
			strcpy(value,"(nil)");
			//return getCommand_from_mysql(command);
		}
		else if( reply->type == REDIS_REPLY_STRING || reply->type == REDIS_REPLY_ERROR ){
			strcpy(value , reply->str);
		}
		else{
				strcpy(value,"(null)");
		}
	}
	freeReplyObject(reply);
}




/* Set new value into mysql*/
void setCommand_to_mysql(char * command, char * status){

	char key[key_size],value[value_size];
	if(is_mysql_up){
		parse_set_command(command,key,value);
		char statement[512];
		char *ttl = "-1";
		snprintf(statement, sizeof(statement) , "INSERT INTO redisStore(keyID,value,ttl) VALUES (('%s'),('%s'),('%s')) ON DUPLICATE KEY UPDATE value = ('%s');" , key , value, ttl, value );

		if(mysql_query(mySQLcon, statement) == 0 ){
			//successful execution
			strcpy(status , "OK");
		}
		else{
			is_mysql_up = False;
			strcpy(status , "MySQL Error");
		}	
	}
	else{
		strcpy(status , "MySQL Error");
		// do nothing
		// to do in future maintain persistent queue or any other mechanism
	}
}

/* Main function to execute GET command */
void setCommand( char *command , char * status){

	redisReply *reply;	
	reply = redisCommand(context, command);

	if( reply == NULL ){				// redis down
			strcpy(status, "Redis down.");
			is_redis_up = False;
			// todo future scope maintain persistent queue or any other mechanism. To send command to sql or not?? 
	}

	else{			//redis up and functioning
					//execute set command on mySQL
		setCommand_to_mysql(command,status);		
	}

	freeReplyObject(reply);
}

void expireCommand_to_mysql(char *command, char * status){
	char key[key_size];
	long long ttl=0;
	char char_ttl[128];
	
	if(is_mysql_up){

		parse_expire_command(command,key,&ttl);
		ttl  = ttl + time(NULL);
		char statement[512];
		sprintf(char_ttl, "%lld", ttl);
		snprintf(statement, sizeof(statement) , "UPDATE redisStore set ttl = ('%s') where keyID = ('%s')" , char_ttl , key );

		if(mysql_query(mySQLcon, statement) == 0 ){
			//successful execution
			strcpy(status , "OK");
		}
		else{
			is_mysql_up = False;
			strcpy(status , "MySQL Error");
		}	
	}
	else{
		strcpy(status , "MySQL Error");
		// do nothing
		// to do in future maintain persistent queue or any other mechanism
	}
}

void expireCommand(char * command, char * status){
	redisReply *reply;
	reply = redisCommand(context, command);

	if( reply == NULL ){				// redis down
			strcpy(status , "Redis down.");
			is_redis_up = False;
			// todo future scope maintain persistent queue or any other mechanism. To send command to sql or not?? 
	}
	else{			//redis up and functioning
					//execute set command on mySQL
		expireCommand_to_mysql(command,status);		
	}

	freeReplyObject(reply);
}

/* function executing command one by one */
void execute_command(char * command){
	char result[2048];
		switch(check_command_type(command)){
			case SET:
						setCommand(command,result); 
						printf("%s\n",result);
						break;
			case GET:
						getCommand(command,result);
						printf("%s\n",result);
						break;
			case EXPIRE:
						expireCommand(command,result);
						printf("%s\n",result);
						break;
			case INVLD_CMD:
						printf("Invalid Command\n");
		}
}

void start_redis_server(){
	pthread_t redis_state_thread;
	pthread_create( &redis_state_thread, NULL, check_redis_server_state, NULL );
	//pthread_join( redis_state_thread , NULL);
}

void start_mysql_server(){
	pthread_t mysql_state_thread;
	pthread_create( &mysql_state_thread, NULL, check_mysql_server_state, NULL );
	//pthread_join( mysql_state_thread , NULL);
}

int main(int argc, char ** argv){
	if(argc != 2){
		printf("Usage ./customProxy <command_file_name>\n");
		exit(1);
	}

    FILE * fp;
    char * line = NULL;
    size_t len = 0;
    ssize_t read;

	start_redis_server();
	start_mysql_server();
	printf("Setting up servers...\n");
	sleep(10);
	printf("Ready to go.\n");
	fp = fopen(argv[1], "r");

       if (fp == NULL){
           exit(1);
       }

       while ((read = getline(&line, &len, fp)) != -1) {
       		int len = strlen(line);
       		if(len>0 && line[len-1] == '\n')
       			line[len-1] = 0;

           execute_command(line);
       		//printf("%s",line );
       }
       fclose(fp);
       
       if (line){
           free(line);
       }
	return 0;
}