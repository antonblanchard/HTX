/* IBM_PROLOG_BEGIN_TAG */
/* 
 * Copyright 2003,2016 IBM International Business Machines Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/* IBM_PROLOG_END_TAG */
/* @(#)38	1.8  src/htx/usr/lpp/htx/bin/htxd/htxd_daemon.c, htxd, htxfedora 9/15/15 20:28:01 */



#include <stdio.h>


#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <errno.h>

#ifndef __HTX_LINUX__    /** AIX **/
 #include <sys/types.h>
 #include <sys/socket.h>
 #include <netdb.h>
 #include <arpa/inet.h>
#endif

#include "htxd_socket.h"
#include "htxd_signal.h"
#include "htxd_option_methods.h"
#include "htxd.h"
#include "htxd_instance.h"
#include "htxd_define.h"
#include "htxd_thread.h"
#include "htxd_trace.h"
#include "htxd_util.h"

extern volatile int htxd_shutdown_flag;

extern int htxd_shutdown_all_mdt(void);
extern int htxd_stop_hotplug_monitor(htxd_thread **);
extern int htxd_autostart(htxd *);
extern int detach_syscfg(void);

int htxd_verify_command_buffer_format(char *command_buffer)
{

	if(command_buffer == NULL) {
		HTXD_TRACE(LOG_ON, "htxd_verify_command_buffer_format: command_buffer is NULL");
		return -1;
	}

	if(strlen(command_buffer) == 0) {
		HTXD_TRACE(LOG_ON, "htxd_verify_command_buffer_format: commnad buffer length is 0");
		return -2;
	}

	if(command_buffer[0] != ':') {
		return -3;
	}

	return 0;
}


void *htxd_command_thraead_handler(void *ptr_socket)
{

	char *		command_buffer		= NULL;
	htxd_command	command_object;
	int		command_return_code	= 0;
	int		client_socket;
	char *		command_result		= NULL;
	char		trace_string[256];
	int		result			= 0;


	client_socket = *( (int *)ptr_socket);	
	free(ptr_socket);
	/* receive the incoming command */
	HTXD_TRACE(LOG_OFF, "daemon receiving command");
	command_buffer = htxd_receive_command(client_socket);
	if(command_buffer == NULL)
	{
		sprintf(trace_string, "Error : htxd_receive_command: returned NULL");
		HTXD_TRACE(LOG_ON, trace_string);
		return NULL;
	}

	HTXD_TRACE(LOG_OFF, "command received start<<");
	HTXD_TRACE(LOG_OFF, command_buffer);
	HTXD_TRACE(LOG_OFF, ">> command received end");

	if(htxd_verify_command_buffer_format(command_buffer) != 0) {
		HTXD_TRACE(LOG_ON, "htxd_start_daemon: htxd_verify_command_buffer_format failed for the command buffer");
		HTXD_TRACE(LOG_ON, "further processing of the command will be ignored");

		sprintf(trace_string, "Error while receiving command, errno <%d>", errno);
		result = htxd_send_response(client_socket, trace_string, 1, -1);
		if(result == -1) {
			HTXD_TRACE(LOG_ON, "1. htxd_send_response() is returned -1, ignoring further processing of the command");
		}
		HTXD_TRACE(LOG_ON, trace_string);
		close(client_socket);
		if(command_buffer != NULL) {
			free(command_buffer);
			command_buffer = NULL;
		} 
		
		return NULL;
	} 

	htxd_update_command_object(command_buffer, &command_object);

	if(command_buffer != NULL) {
		free(command_buffer);
		command_buffer = NULL;
	} 

	/* process the received command */
	HTXD_TRACE(LOG_OFF, "daemon start processing command");
	command_return_code = htxd_process_command(&command_result, &command_object);

	/* handling if command did not generate result buffer */		
	if(command_result == NULL) {
		command_result = malloc(HTX_ERR_MESSAGE_LENGTH);
		if(command_result == NULL) {
			sprintf(trace_string, "Error : malloc(%d) failed with errno = <%d> while allocating error message", HTX_ERR_MESSAGE_LENGTH, errno);
			HTXD_TRACE(LOG_ON, trace_string); 
			exit(1);
		}
		strcpy(command_result, "No result is generated by the command");
	}

	HTXD_TRACE(LOG_OFF, "command result start<<");
	HTXD_TRACE(LOG_OFF, command_result);
	HTXD_TRACE(LOG_OFF, ">> command result end");

	/* send back command result to client */
	HTXD_TRACE(LOG_OFF, "daemon sending the result to client");
	result = htxd_send_response(client_socket, command_result, command_object.command_type, command_return_code);
	if(result == -1)
	{
		sprintf(trace_string, "Error : htxd_send_response returned with -1");
		HTXD_TRACE(LOG_ON, trace_string); 
		return NULL;
	}
	
	if(command_result != 0) {
		free(command_result);
	}

	close(client_socket);

	htxd_exit_command_thread();	

	return NULL;
}



/* daemon live here, receives command, process it, send back result */
int htxd_start_daemon(htxd *htxd_instance)
{

	int			result				= 0;
	int			socket_fd;
	struct sockaddr_in	local_address;
	struct sockaddr_in	client_address;
	socklen_t		address_length;
	int			client_socket_fd;
	htxd_thread		command_thread;



	HTXD_FUNCTION_TRACE(FUN_ENTRY, "htxd_start_daemon");

	init_option_list();
	htxd_autostart(htxd_instance);  /* try autostart if find the autostart flag file */

	if(htxd_is_profile_initialized(htxd_instance) != TRUE) {
		HTXD_TRACE(LOG_ON, "initialize HTX profile details");
		htxd_init_profile(&(htxd_instance->p_profile));
		/* htxd_display_profile(htxd_instance->p_profile);  */ /* To DEBUG */
	}

	socket_fd = htxd_create_socket();

	result = htxd_set_socket_option(socket_fd);

	local_address.sin_family = AF_INET;
	local_address.sin_port = htons (htxd_instance->port_number);
	local_address.sin_addr.s_addr = INADDR_ANY;
	memset (&(local_address.sin_zero), '\0', 8);


	result = htxd_bind_socket(socket_fd, &local_address, htxd_instance->port_number);

	result = htxd_listen_socket(socket_fd);

	HTXD_TRACE(LOG_ON, "starting daemon main loop");
	do  /* this loop make the daemon live */
	{
		if(htxd_shutdown_flag == TRUE) {
			break;
		}

		client_socket_fd = htxd_accept_connection(socket_fd, &client_address, &address_length);
		if(client_socket_fd == -1)
		{
			if(htxd_shutdown_flag == TRUE) {
				break;
			}
			HTXD_TRACE(LOG_ON, "htxd_accept_connection returned -1, ignoring and continue...");
			continue;
		}

		HTXD_TRACE(LOG_OFF, "found a command for receiving");
	
		memset(&command_thread, 0, sizeof(htxd_thread));
		command_thread.thread_function = htxd_command_thraead_handler;
		command_thread.thread_data = malloc(sizeof(int));
		(*((int *)command_thread.thread_data)) = client_socket_fd;
		command_thread.thread_stack_size = 10000000;
	
		htxd_create_command_thread(&command_thread);	


	} while(htxd_shutdown_flag == FALSE);

	htxd_shutdown_all_mdt();

	HTXD_FUNCTION_TRACE(FUN_EXIT, "htxd_start_daemon");

	return result;
}



void htxd_cleanup_system_shm(void)
{
	int shm_id;
	void *shm_address;

	shm_id = htxd_get_exer_table_shm_id();
	shm_address = (void *) htxd_get_exer_table();
	htxd_cleanup_shm(shm_id, shm_address);

	shm_id = htxd_get_system_header_info_shm_id();
	shm_address = (void *) htxd_get_system_header_info();
	htxd_cleanup_shm(shm_id, shm_address);

	htxd_delete_ecg_manager();

}



/* putting daemon back to idle state */
int htxd_idle_daemon(void)
{
	int return_code;
	char trace_string[256];
	pid_t htx_stats_pid;
	pid_t htx_msg_pid;
	pid_t htx_equaliser_pid;
	htxd *htxd_instance;
#ifdef __HTXD_DR__
	pid_t htx_dr_child_pid;
#endif

	
	if(htxd_is_hang_monitor_initialized() == TRUE) {
		htxd_instance = htxd_get_instance();
		htxd_stop_hang_monitor(&(htxd_instance->p_hang_monitor_thread));
		htxd_remove_hang_monitor();

		sprintf(trace_string, "stopping hang monitor thread");
		HTXD_TRACE(LOG_OFF, trace_string);
	}

	if(htxd_is_stop_watch_monitor_initialized() == TRUE) {
		htxd_instance = htxd_get_instance();
		htxd_stop_stop_watch_monitor(&(htxd_instance->stop_watch_monitor_thread));
		htxd_remove_stop_watch_monitor();

		sprintf(trace_string, "stopping stop watch monitor thread");
		HTXD_TRACE(LOG_OFF, trace_string);
	}
	
	htx_stats_pid = htxd_get_htx_stats_pid();
	if (htx_stats_pid != 0) {
		htxd_send_SIGTERM(htx_stats_pid);

		sprintf(trace_string, "sent SIGTERM to hxstats process, pid <%d>", htx_stats_pid);
		HTXD_TRACE(LOG_OFF, trace_string);
	}

#ifdef __HTX_LINUX__
	if(htxd_is_hotplug_monitor_initialized() == TRUE) {
		htxd_instance = htxd_get_instance();
		htxd_stop_hotplug_monitor(&(htxd_instance->p_hotplug_monitor_thread));
		htxd_remove_hotplug_monitor();

		sprintf(trace_string, "stopping hotplug  monitor thread");
		HTXD_TRACE(LOG_OFF, trace_string);
	}
#endif


#ifdef __HTXD_DR__
	htx_dr_child_pid = htxd_get_dr_child_pid();
	if (htx_dr_child_pid != 0) {
		htxd_send_SIGTERM(htx_dr_child_pid);

		sprintf(trace_string, "sent SIGTERM to DR child process, pid <%d>", htx_dr_child_pid);
		HTXD_TRACE(LOG_OFF, trace_string);
	}
#endif

	htx_equaliser_pid = htxd_get_equaliser_pid();
	if(htx_equaliser_pid != 0) {
		htxd_send_SIGTERM(htx_equaliser_pid);

		sprintf(trace_string, "sent SIGTERM to equaliser process, pid <%d>", htx_equaliser_pid);
		HTXD_TRACE(LOG_OFF, trace_string);
	}
	
	htxd_send_message ("Final message from test: System into idle state", 0, HTX_SYS_INFO, HTX_SYS_FINAL_MSG);

	sprintf(trace_string, "sent test final message to hxsmsg process");
	HTXD_TRACE(LOG_OFF, trace_string);

	while(1) {
#ifdef __HTXD_DR__
		if( (htx_stats_pid == 0) &&  (htx_dr_child_pid == 0) ) {
			break;
		}
		htx_stats_pid = htxd_get_htx_stats_pid();
		htx_dr_child_pid = htxd_get_dr_child_pid();
		sleep(1);
#else
		if(htx_stats_pid == 0) {
			break;
		}

		htx_stats_pid = htxd_get_htx_stats_pid();
		sleep(1);
#endif

	}

	while(1) {
		htx_msg_pid = htxd_get_htx_msg_pid();
		if(htx_msg_pid == 0) {
			sprintf(trace_string, "hxsmsg is stopped");
			HTXD_TRACE(LOG_OFF, trace_string);
			break;
		}
		sleep(1);
		sprintf(trace_string, "wating for exiting hxsmsg process, pid <%d>", htx_msg_pid);
		HTXD_TRACE(LOG_ON, trace_string);
	}

	while(1) {
		htx_equaliser_pid = htxd_get_equaliser_pid();
		if(htx_equaliser_pid == 0) {
			sprintf(trace_string, "equaliser is stopped");
			HTXD_TRACE(LOG_OFF, trace_string);
			break;
		}
		sleep(1);
		sprintf(trace_string, "wating for exiting equaliser process, pid <%d>", htx_equaliser_pid);
		HTXD_TRACE(LOG_ON, trace_string);
	}

	if (htxd_is_init_syscfg() == TRUE) {
		return_code = detach_syscfg();
		if(return_code != 0) {
			sprintf(trace_string, "Internal error: failed to detach syscfg with error code <%d>", return_code);
			HTXD_TRACE(LOG_ON, trace_string);
		}
		htxd_set_init_syscfg_flag(FALSE);
	}

	htxd_cleanup_system_shm();
	htxd_set_daemon_state(HTXD_DAEMON_STATE_IDLE);
	htxd_set_test_running_state(HTXD_TEST_HALTED);

	sprintf(trace_string, "daemon is in idle state now");
	HTXD_TRACE(LOG_OFF, trace_string);

	return 0;
}
