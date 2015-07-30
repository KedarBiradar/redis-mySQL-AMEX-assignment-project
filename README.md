##Functioning
	Following redis commands are implemented successfully with mysql as backend server for persistent storage.
	1. GET key
	2. SET key value
	3. EXPIRE key ttl

	where
		key and value are string and ttl is time in seconds.

##Files
	1. customProxy.h
		- header file for all functions

	2. parse_commands.c
		- contains code for parsing the redis command that has to be execute on mysql server.

	3. customProxy.c
		- Contains code for executing SET,GET,EXPIRE commands of redis.
		- By default it connects to local redis server "127.0.0.1" with port number "6379" and local mysql server "127.0.0.1" using user as "root" and password as "12345".
		- If you want to change redis server or mysql server then just change the variable values in this file located at top in customProxy.c.


##Compiling
	gcc -o customProxy customProxy.c parse_commands.c libhiredis.a -std=c99  `mysql_config --cflags --libs` -Wall


##Running
	./customProxy <command_file_name>

