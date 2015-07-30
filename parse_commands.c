#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <my_global.h>
#include <mysql.h>
#include "hiredis.h"
#include "customProxy.h"

/* check incoming command name like get, set etc */
int check_command_type(char * command)
{
	
	int command_length=0;
	int i = 0;
	int j = 0;
	char cmd[32];
	char delim = ' ';
	command_length= strlen(command);

	i = 0;
	while(i < command_length && command[i] == delim){
		i++;
	}
	while(i<command_length && command[i] != delim){
		cmd[j]=command[i];
		j++;
		i++;
	}
	cmd[j]=0;
	
	if( strcasecmp(cmd,"SET") == 0 )
		return SET;
	else if( strcasecmp(cmd,"GET") == 0 )
		return GET;
	else if( strcasecmp (cmd,"EXPIRE") == 0)
		return EXPIRE;
	else
		return INVLD_CMD;
}

/* parse set command */
void parse_set_command(char * command,char * key,char * value){
	int i,j;
	i = 0;
	j = 0;
	// skip all white spaces
	while(command[i] && command[i] == ' '){
		i++;
	}

	// skip command name
	while(command[i] && command[i] != ' '){
		i++;
	}

	// skip all white spaces
	while(command[i] && command[i] == ' '){
		i++;
	}

	// copy key from command to key field
	while(command[i] && command[i] != ' '){
		key[j++] = command[i++];
	}
	key[j] = 0;
	j = 0;

	// skip all white spaces
	while(command[i] && command[i] == ' '){
		i++;
	}

	// copy value from command to value field
	while(command[i] && command[i] != ' '){
		value[j++] = command[i++];
	}
	value[j] = 0;
}

/* parse set command */
void parse_expire_command(char * command,char * key,long long * ttl){
	int i,j;
	i = 0;
	j = 0;
	char value[128];
	// skip all white spaces
	while(command[i] && command[i] == ' '){
		i++;
	}

	// skip command name
	while(command[i] && command[i] != ' '){
		i++;
	}

	// skip all white spaces
	while(command[i] && command[i] == ' '){
		i++;
	}

	// copy key from command to key field
	while(command[i] && command[i] != ' '){
		key[j++] = command[i++];
	}
	key[j] = 0;
	j = 0;

	// skip all white spaces
	while(command[i] && command[i] == ' '){
		i++;
	}

	// copy value from command to value field
	while(command[i] && command[i] != ' '){
		value[j++] = command[i++];
	}
	value[j] = 0;
	*ttl = atoll(value);
}

void parse_get_command(char * command,char * key){
	char *ch;
	ch = strtok(command, " ");
	ch = strtok(NULL, " ");
	int i = 0;
	while(*ch){
		key[i++]=(*ch);
		ch++;
	}
	key[i]=0;
}
