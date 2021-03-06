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

/* @(#)18	1.5  src/htx/usr/lpp/htx/bin/hxsmsg/open_file.c, htx_msg, htxubuntu 5/24/04 18:16:53 */

/*
 *   FUNCTIONS: open_file
 */


#include <htx_local.h>

#ifdef __HTX_LINUX__
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#endif /* __HTX_LINUX__ */
 

/*
 * NAME: open_file()
 *                                                                    
 * FUNCTION: Opens the specified file and checks for errors.
 *                                                                    
 * EXECUTION ENVIRONMENT:
 *                                                                   
 *	This function is called by the check_logs() and open_logs() functions
 *      of the "hxsmsg" program.
 *
 *      The message handler program, "hxsmsg", is always a child process
 *      of the HTX supervisor program, "hxssup".
 *
 * NOTES: 
 *
 *      operation:
 *      ---------
 *
 *      open file
 *      check for errors
 *
 *      return
 * 
 *               
 * RETURNS: 
 *
 *      Return Codes:
 *      -------------------------------------------------
 *          -1 -- Error on open() system call.
 *      x >= 0 -- file ID returned by open() system call.
 *
 *
 */  

int open_file(char *filename, int oflag, int mode)
     /*
      * filename -- the name of the file to be opened.
      * oflag -- the open flag (sets open() parameters)
      * mode -- specifies file permissions
      */

{
  /*
   ***  Data and Functions Definitions/Declarations  **************************
   */
  	char error_msg[128];       /* error message string                    */

  	extern char *program_name; /* this program's name (argv[0])           */

  	int errno_save;            /* errno save area                         */
  	int fileid;                /* file ID returned by open() system call  */

  /*
   ***  Beginning of Executable Code  *****************************************
   */
  	errno = 0;
  	if ((fileid = open(filename, oflag, mode)) == -1)
    	{
      		errno_save = errno;

      		(void) sprintf(error_msg, 
		"\n%s -- Error on open() for %s.\nerrno: %d (%s).\n",
		program_name,
		filename,
		errno_save,
		strerror(errno_save));

      		(void) fprintf(stderr, "%s", error_msg);
      		(void) fflush(stderr);
    	} /* endif */

  	return(fileid);

} /* open_file() */

