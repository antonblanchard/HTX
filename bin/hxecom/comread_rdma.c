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
/* @(#)90       1.30.1.2  src/htx/usr/lpp/htx/bin/hxecom/comread.c, exer_com, htx610 8/1/07 07:16:53 */
/*
 *   COMPONENT_NAME: exer_com
 *
 *   FUNCTIONS:
 *		comread_rdma
 *      do_compare
 *      ExitCleanup
 *
 */


#   include <stdio.h>
#   include <stdlib.h>
#   include <unistd.h>
#   include <string.h>
#   include <memory.h>
#   include <sys/types.h>
#   include <sys/socket.h>
#   include <sys/time.h>
#   include <sys/select.h>
#   include <sys/ioctl.h>
#   include <netinet/in.h>
#   include <netdb.h>
#   include <ctype.h>
#   include "hxecomdef.h"
#   include "comrw.h"
#	include "comdapl.h"


static int dapl_read_event(struct dapl_device *dev, unsigned long long xfer_len, struct htx_data *stats);
static void ExitCleanup();

/* define NO_BLOCK 1 */

#ifdef __HTX_LINUX__
#define NFDS(a)   (a)
#endif

static void SendResetToSemServ(int WriteIdx, int WriteAhead,unsigned char timestamp, int i, int bsize, int stanza, int pak,
                                struct sockaddr_in * dest, struct htx_data * stats, u_long state);
static int ReqPktSize(int WriteIdx, u_short bufmin, u_short bufmax, u_short num_oper, u_short * bsize, int * num_pktsizes,
            struct htx_data * stats, struct sockaddr_in dest, u_long state );
static void RWmsg(struct cum_rw * Lstats, struct rule_format * rule, int Stanza, struct htx_data * stats, int err, enum sev_code sev, char * msg_text);
static int  headerCkFail(char rbuf[], char MsgStr[]);
static int  not_running_bootme(void);
static int do_compare(char *rbuf_ptr,char *pattern_ptr,int pak_size, struct rule_format *rule, struct htx_data *stats,char ConnectStr[],int fixpat);
static void SendTermToSemServ(int WriteIdx, struct sockaddr_in * dest, struct htx_data * stats, u_long state);
static void SendHaltToSemServ(int WriteIdx, struct sockaddr_in * dest, struct htx_data * stats, u_long state);
static void bad_bytes_read(struct cum_rw * Lstats, struct htx_data * pstats,
                      struct htx_data * stats, int bytes, int BadCnt);
static int SRead(SOCKET fd, register char *ptr, register int nbytes, int nmax, struct rw_t * rwParms,struct htx_data * stats, char *connect_str);
static int DRead(SOCKET fd, register char *ptr, register int nbytes, int nmax, struct rw_t * rwParms,struct htx_data * stats,char *connect_str);
static void ReadExit(struct htx_data * stats, struct cum_rw * LSTATS, int NoStanzas, int exitNo,
                                    char msg[], char ConnectStr[]);
static void good_bytes_read(struct cum_rw * Lstats, struct htx_data * pstats,
                                     struct htx_data * stats, int bytes, char ConnectStr[]);
static void SendReadAckToSemServ(struct CoordMsg * CMsg, struct sockaddr_in * dest, struct htx_data * stats, u_long state);
static int SendAddrExcToSemServ(struct dapl_device *dev, DAT_RMR_TRIPLET *rmr_msg, int WriteIdx, 
												struct sockaddr_in *dest, struct htx_data *stats, u_long state);

                    
#define MISC 0xBEEFDEAD
#define VER 0x10140900
#pragma mc_func trap { "7c810808" }
#pragma reg_killed_by trap

struct dapl_device *dapl_instance_ptr = NULL;

/* global vars */
char writer_dev[40];
static pid_t pid = 0;
void *comread_rdma(void * reader_arg, int exer_idx)
{
	DAT_RETURN ret;
	DAT_DTO_COOKIE cookie;
	int i, rc, stanza, num_stanza, pak = 0, o = 0, index = -1;
	char * strt_rbuf = NULL,  *rbuf = NULL ;
	unsigned int pattern_max = 0;
	struct comread_argt *arg = NULL; 
	struct rule_format rule[MAX_STANZAS];
	char connect_str[CONNECTSTR_LEN];
	socklen_t addrlen; 
	SOCKET ReaderTestSock; 
	struct sockaddr_in *server_sockaddr = NULL;
	struct sockaddr_in reader_sockaddr;
	struct htx_data *stats = NULL;
	unsigned long long msg_type; 
	unsigned char   timestamp = '\0';
	struct cum_rw * LSTATS;
	u_short bsize = 0, pakLen = 0; 	
	int WriterIdx; 
	char msg_text[1024];

	arg = (struct comread_argt *)reader_arg;
	stats = &arg->stats;
	ReaderTestSock = arg->ReaderTestSock; 
	WriterIdx = arg->WriterIdx;
	atexit(ExitCleanup);

	server_sockaddr = &shm_HXECOM->exer[shm_pHXECOM->ExerNo].ServerID.sock;

	/********************************************************************/
	/* Get ReaderID -- to be used for ConnectStr.                       */
	/********************************************************************/
	#ifdef __RDMA_SUPPORTED___
	addrlen = sizeof(reader_sockaddr);
    if(getsockname(ReaderTestSock, (struct sockaddr *) &reader_sockaddr, &addrlen)) {
        sprintf(msg_text, "comread: Error getsockname() - %s.\n", STRERROR(errno));
        hxfmsg(stats, HTXERROR(EX_SOCK2,ERRNO), HTX_HE_SOFT_ERROR, msg_text);
        HE_exit(EX_SOCK2);
    }
	#else 
	memcpy(&reader_sockaddr, &arg->ReaderID, sizeof(struct sockaddr_in));
	#endif
	/********************************************************************/
	/* Obtain rules file stanzas as array of struct rule_format         */
	/********************************************************************/
	num_stanza = GetRules(*server_sockaddr, rule, MAX_STANZAS, stats);

	/********************************************************************/
	/* Find largest pattern to be read.                                 */
	/********************************************************************/
	for(i = 0; i < num_stanza; i++) {
		if (rule[i].bufmax > pattern_max) {
			pattern_max = rule[i].bufmax;
		}
	}
	/********************************************************************/
	/* Allocate buffer for pkt sizes                                    */
	/********************************************************************/
    if ((strt_rbuf = (char *)malloc(pattern_max + 2 * (TP_HDR_LEN) + DAT_OPTIMAL_ALIGNMENT)) == NULL) {
        sprintf(msg_text, "comread_rdma: Malloc of read buffer space failed - %s\n", STRERROR(errno));
        hxfmsg(stats, HTXERROR(EX_MALLOC6,ERRNO), HTX_HE_SOFT_ERROR, msg_text);
        HE_exit(EX_MALLOC6);
    }

	memset(strt_rbuf, 0xBB, pattern_max + 2 * (TP_HDR_LEN) + DAT_OPTIMAL_ALIGNMENT); 
    /********************************************************************/
    /* Align the send/recv buffer to DAT_OPTIMAL_ALIGNMENT              */
    /********************************************************************/
    if(DAT_OPTIMAL_ALIGNMENT) {
        rbuf = (char *)((unsigned long long)((unsigned long long)strt_rbuf + (unsigned long long)(DAT_OPTIMAL_ALIGNMENT - 1)) &
                       (unsigned long long)(~((unsigned long long)(DAT_OPTIMAL_ALIGNMENT - 1))));
    } else {
        rbuf = strt_rbuf;
    }

   	
	/********************************************************************/
	/* Allocate space for local stats and zero.                         */
	/********************************************************************/
    if((LSTATS = (struct cum_rw *)malloc(sizeof(struct cum_rw) * num_stanza)) == NULL) {
        sprintf(msg_text, "comread: Malloc of local stats space failed - %s\n", STRERROR(errno));
        hxfmsg(stats, HTXERROR(EX_MALLOC8,ERRNO), HTX_HE_SOFT_ERROR, msg_text);
        HE_exit(EX_MALLOC8);
    }
	memset(LSTATS, 0, (sizeof(struct cum_rw) * num_stanza)); 
   	/********************************************************************/
	/* Obtain string used for ID when writing messages.                 */
	/********************************************************************/

    GetConnectStr(connect_str, CONNECTSTR_LEN, "R", reader_sockaddr, " connected to W", arg->WriterTestSock, stats);

	printf("Reader Task[%d] doing dapl_init\n", getpid());
	/***************************************************************************/ 
	/* This function looks at /etc/rdma/dat.conf and find provider. 		   */
	/* Also opens the interface, creates protection zone and creates events.   */ 
	/***************************************************************************/
	ret = dapl_init_instance(stats->sdev_id + 5, WriterIdx, ntohs(reader_sockaddr.sin_port), &dapl_instance_ptr, stats, rule[0].alarm);
	if (ret != DAT_SUCCESS) {
		sprintf(msg_text, "comread: dapl_init_instance failed errno = %d, ret = %d \n", errno, ret); 
		hxfmsg(stats, HTXERROR(EX_DAPL_INIT, ERRNO), HTX_HE_SOFT_ERROR, msg_text);
   		HE_exit(EX_DAPL_INIT);
	}
	/*********************************************************************/
	/* Register reference buffer = rbuf. Reader for now is dummy entity  */
	/* it only posts rbuf for receiving RDMA_WRITE data from writer and  */ 
	/* use the same buffer for RDMA_READ intiated by writer 			 */ 
	/* TODO : check the feasibilty of REMOTE NOTIFICATION, if possible   */
	/* reader can compare RDMA_WRITE data. ---> Removed for now. 		 */ 
	/*********************************************************************/
	ret = dapl_register_mem(dapl_instance_ptr, rbuf, (pattern_max +  2 * (TP_HDR_LEN)), &dapl_instance_ptr->rcv_mr, stats);
	if (ret != DAT_SUCCESS) {
		sprintf(msg_text, "comread: dapl_register_mem failed for rbuf=0x%x, ret=%d, errno=%d \n", rbuf, ret, errno); 
		hxfmsg(stats, HTXERROR(EX_DAPL_REGISTER, ERRNO), HTX_HE_SOFT_ERROR, msg_text);
   		HE_exit(EX_DAPL_REGISTER);
	}

    /**********************************************************************/
    /* Become a active server                                            **/
    /**********************************************************************/
    ret = dapl_listen_ep(dapl_instance_ptr, &dapl_instance_ptr->event, stats);
    if (ret != DAT_SUCCESS) {
        sprintf(msg_text, "comread:dapl_listen_ep ret=%d, errno=%d \n", ret, errno);
        hxfmsg(stats, HTXERROR(EX_DAPL_LISTEN, ERRNO), HTX_HE_SOFT_ERROR, msg_text);
        HE_exit(EX_DAPL_LISTEN);
    }

	printf("#########  DAPL Reader listening on port = 0x%x to accept connection\n", dapl_instance_ptr->conn_id);
	
	/**********************************************************************/ 
	/* Unblock our writer. 												  */ 
	/**********************************************************************/
	SendResetToSemServ(WriterIdx, rule[0].write_ahead, timestamp, 1, 2, 3, 4, &arg->SemServerSock, stats, 0); 	

	printf("Reader waiting to accept connection\n");
	/*********************************************************************/
	/* Connect 															 */ 
	/*********************************************************************/
	ret = dapl_accept_ep(dapl_instance_ptr, &dapl_instance_ptr->event, NULL, 0, stats);
	if (ret != DAT_SUCCESS) {
		sprintf(msg_text, "comread: dapl_accept_ep ret = %d, errno = %d \n", ret, errno); 
		hxfmsg(stats, HTXERROR(EX_DAPL_ACCEPT, ERRNO), HTX_HE_SOFT_ERROR, msg_text);	
    	HE_exit(EX_DAPL_ACCEPT);
	}
#ifdef __DEBUG__
	sprintf(msg_text, "Reader : ReaderIdx = %d, Connected ! \n %s \n", WriterIdx, connect_str); 
	hxfmsg(stats, HTXERROR(HEINFO10,0), HTX_HE_INFO, msg_text);
#endif 

	/**************************************************************************/
	/* Local iov = rbuf, so RDMA_WRITE data would be recvd in rbuf.    
	 * rbuf is used for RDMA_READ to send data.  
	 * rbuf_info = Writer buffer where we copy RDMA READ data				
	 **************************************************************************/ 
	ret = SendAddrExcToSemServ(dapl_instance_ptr, &dapl_instance_ptr->rbuf_info, WriterIdx, &arg->SemServerSock, stats, 0);
	if (ret != DAT_SUCCESS) {
		sprintf(msg_text, "comread: dapl_addr_exchange failed ret = %d, errno = %d \n", ret, errno); 
		hxfmsg(stats, HTXERROR(EX_DAPL_ADDR_XCHG, ERRNO), HTX_HE_SOFT_ERROR, msg_text);
    	HE_exit(EX_DAPL_ADDR_XCHG);
	}
#ifdef __DEBUG__
	sprintf(msg_text, "Reader: ReaderIdx = %d, \n RDMA_WRITE recv_buffer = 0x%llx, rcv_mr = %d \n" 
									  				 " RDMA_READ  send_buffer = 0x%llx, context = %d \n",  WriterIdx, rbuf, 
							 dapl_instance_ptr->rcv_mr.lmr_ctx, dapl_instance_ptr->rbuf_info.virtual_address, dapl_instance_ptr->rbuf_info.rmr_context);
	hxfmsg(stats, HTXERROR(HEINFO10,0), HTX_HE_INFO, msg_text);
#endif 
	sprintf(msg_text, "Reader:%d Beginning RDMA read events tests..., pid=%d", WriterIdx, getpid());
    hxfmsg(stats, HTXERROR(HEINFO10,0), HTX_HE_INFO, msg_text);


    while(1) {
		for(stanza = 0; stanza < num_stanza; stanza++) {
			/* Rules timeout is in seconds, dat needs in microseconds, Convert ... */ 
			dapl_instance_ptr->alarm = rule[stanza].alarm * 1000 * 1000;
           	stats->test_id = stanza + 1;
           	hxfupdate(UPDATE, stats);

            for(bsize = rule[stanza].bufmin; bsize <= rule[stanza].bufmax; bsize += rule[stanza].bufinc) {
                pakLen =  bsize + 2 * (TP_HDR_LEN);

                for(i=0; i<rule[stanza].num_oper; i++) {

                    if(shm_pHXECOM->SigTermFlag) {
						/********************************************/
                        /* Give writer a chance to terminate before */
                        /* we shutdown program.                      */
                        /********************************************/
                        SendTermToSemServ(WriterIdx, &arg->SemServerSock, stats, 0);
						sleep(5);
                        hxfupdate(UPDATE, stats);
                        sprintf(msg_text, "Fast socket shutdown due to SIGTERM signal.\n");
						hxfmsg(stats, ret, HTX_HE_INFO, msg_text);
                        HE_exit(0);    
                   	}
					/* Since we donot have Remote Notification enabled with uDAPL, 
				 	 * do an hxfupdate here, so that SUP doesnot detects a hang from us. 
					 * Reader process needs to be there, so as to keep remote buffer valid
					 * throughout writer's life time. 
					 */ 
					usleep(dapl_instance_ptr->alarm); 
				 	hxfupdate(UPDATE, stats);
					
				} /* END of num_oper loop */
			} /* END of bsize loop */  
       } /* END of num_stanza loop */

    } /* END OF while(1) */
    return 0;
}




static void ReadExit(struct htx_data * sstats, struct cum_rw * LSTATS, int NoStanzas, int exitNo,
                                    char msg[], char ConnectStr[])
{
    sprintf(msg + strlen(msg), "%s\nStats Updated. Exiting.\n", ConnectStr);
    hxfmsg(sstats, exitNo, HTX_HE_INFO, msg);
    HE_exit(exitNo>>16);
}


static int headerCkFail(char rbuf[], char MsgStr[])
{
    int i;
    int error=0;
    int sum = 0;

    for(i=0; i<TP_SEQ_LEN; i++) {
        switch(rbuf[i]) {
            case '-':
            case ' ':
            case '1':
            case '0':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                sum += (unsigned char)rbuf[i];
                break;
            default:
                error = 1;
        }
    }
    if(rbuf[TP_SEQ_LEN] != '\0')
        error = 1;
    sum += (unsigned char)rbuf[TP_TIME_POS];
    if((0xff & sum) != (unsigned char)rbuf[TP_HDR_LEN -1])
        error = 1;
    if(error) {
        sprintf(MsgStr, "Header Error \"");
        for(i=0; i<TP_HDR_LEN; i++)
            /*sprintf(MsgStr + strlen(MsgStr), "%2x%2x", rbuf[i],'\0');*/
            sprintf(MsgStr + strlen(MsgStr), "%2x", rbuf[i]);
        sprintf(MsgStr + strlen(MsgStr), "\"\n");
    }
    return error;
}



static void bad_bytes_read(struct cum_rw * Lstats, struct htx_data * pstats, struct htx_data * sstats,
                             int bytes, int BadCnt)
{
    if(BadCnt == 1) {
        sstats->bad_reads             += 1;
        pstats->bad_reads            += 1;
        Lstats->totals.bad_rw        += 1;
        Lstats->current.bad_rw       += 1;
        sstats->bytes_read            += bytes;
        /*pstats->bytes_read           += bytes; */
        addw(Lstats->totals.bad_bytes_rw1, Lstats->totals.bad_bytes_rw2, bytes);
        addw(Lstats->current.bad_bytes_rw1, Lstats->current.bad_bytes_rw2, bytes);
    }
    else {
        Lstats->totals.bad_others  += 1;
        Lstats->current.bad_others += 1;
        sstats->bad_others          += 1;
        /* pstats->bad_others         += 1; */
    }
}



static void good_bytes_read(struct cum_rw * Lstats, struct htx_data * pstats,
                                     struct htx_data * sstats, int bytes, char ConnectStr[])
{
    if((unsigned int)bytes != Lstats->rw_size) {
		char smsg_text[1024]; 
        sprintf(smsg_text, "%s\nLast rw_size=%d, New rw_size=%d, Must be equal.\n",
                             ConnectStr, Lstats->rw_size, bytes);
        hxfmsg(sstats, HTXERROR(EX_READ15,0), HTX_HE_SOFT_ERROR, smsg_text);
        HE_exit(EX_READ15);
    }
    Lstats->totals.good_rw  += 1;
    Lstats->current.good_rw += 1;
    sstats->good_reads       += 1;
    /* pstats->good_reads      += 1; */
    addw(Lstats->totals.good_bytes_rw1, Lstats->totals.good_bytes_rw2, bytes);
    addw(Lstats->current.good_bytes_rw1, Lstats->current.good_bytes_rw2, bytes);
    sstats->bytes_read       += bytes;
    /* pstats->bytes_read      += bytes; */
}

static void
SendResetToSemServ(int WriteIdx, int WriteAhead, unsigned char timestamp , int i, int bsize, int stanza , int pak,
                    struct sockaddr_in * dest, struct htx_data * stats, u_long state) {

    SOCKET SemServSock;
    struct CoordMsg CMsg;
    char msg_text[100];
    int rc ;

    SemServSock = SetUpConnect(dest, stats, state);
    CMsg.msg_type = htonl(CM_RESET_WRITE);
    CMsg.ID.SemID.WriteIdx   = htons((u_short) 0xffff & WriteIdx);
    CMsg.ID.SemID.WriteAhead = (unsigned char)  0xff & WriteAhead;
    CMsg.ID.SemID.timeS      = (unsigned char)  0xff & timestamp;
    CMsg.ID.SemID.Stanza     = (unsigned char)  0xff & stanza;
    CMsg.ID.SemID.bsize      = htons((u_short) 0xffff & bsize);
    CMsg.ID.SemID.Iloop      = htons((u_short) 0xffff & i);
    CMsg.ID.SemID.ackNo      = htons((u_short) 0xffff & pak);

    rc = StreamWrite(SemServSock, (char *)&CMsg, sizeof(CMsg));
#ifdef __DEBUG__
    sprintf(msg_text, "sending CM_RESET_WRITE to SemServ, rc = %d write_ahead= %d\n", rc, CMsg.ID.SemID.WriteAhead);
    hxfmsg(stats, HTXERROR(EX_WRITE5,ERRNO), HTX_HE_INFO, msg_text);
#endif
    if(rc == -1) {
        sprintf(msg_text, "Error : Sending CM_RESET_WRITE to SemServSock - %s \n", STRERROR(errno));
        hxfmsg(stats, HTXERROR(EX_WRITE5,ERRNO), HTX_HE_SOFT_ERROR, msg_text);
    }

    closesocket(SemServSock);
}



static int do_compare(char *rbuf_ptr,char *pattern_ptr,int pak_size,struct rule_format *rule, struct htx_data *stats,char ConnectStr[],int fixpat)
{
    int a,o;
	int d32, d8;
	/* cast char pointer to 32bit int pointer for 32bit diff */
	const unsigned int *p1 = (uint*)rbuf_ptr, *p2 = (uint*)pattern_ptr; 
	char *debug_ptr;
	char str_pid[16];
	int rc=-1;

    /* used to get max thruput, don't do data comapare for UDP packets */
    if(rule->no_compare || rule->layer == UDP_LAYER) 
		return(-1);

	d32 = pak_size / 4;  /* number of 32bit number */
	d8  = pak_size % 4;  /* remain byte need compare to handle old byte size */

	/* returns -1 if buffers match, so I can return the index to miscompare
	   if there is a miscompare */ 
	/* This version will ignore every 144th byte where the write puts the
       retry count when writes timeout and resend the full packet or part of
	   the packet */  
	if(rule->debug_pattern && fixpat && pak_size >= 512) {
		if(pid == 0) {
			/* this is first packet get the pid and devname */
			/* devname is at index 146 for and pid is at 160 */
			debug_ptr = rbuf_ptr+146;
			strcpy(writer_dev,debug_ptr);	
			debug_ptr = rbuf_ptr+160;
			strcpy(str_pid,debug_ptr);
			pid = atoi(str_pid);
		}
		/* insert the devname and pid in pattern so compare will work. */
		for(o=128; o < pak_size-128; o+=128) {
			debug_ptr = pattern_ptr + o + 16;
			debug_ptr[0]=0x01;  /* put in retry 1, this is what it should be*/
			debug_ptr[1]=0x00;  /* this byte is zeroed by writer */
			debug_ptr += 2;
			/* put in the dev name of writer */
			strcpy(debug_ptr,writer_dev);
			debug_ptr += strlen(writer_dev);
			/* need this to make sure there is no real */
			/* data between dev and pid */
			debug_ptr[0]=0x00;
			debug_ptr[1]=0x00;
			debug_ptr[2]=0x00;
			debug_ptr[3]=0x00;
			debug_ptr = pattern_ptr + o + 32;
			INSERT_PID(debug_ptr,pid);
		}
	}
	printf("Comapring rbuf = 0x%x, wbuf=0x%xi, pak_size = %d \n", rbuf_ptr , pattern_ptr, pak_size); 
	if(memcmp(rbuf_ptr, pattern_ptr, pak_size) != 0) {  
		/* If there is a miscompare I have to return the index into the */
            /* buffer on a char boundary to find that index now */
            for(a=0;a<pak_size;a++) {
                if(rbuf_ptr[a] != pattern_ptr[a]) {
                    return (a);
                }
            }
	} 
	
	return(rc);
}

static void ExitCleanup()
{
    static int cleanup_in_progress = FALSE;

    if (!cleanup_in_progress) {
        cleanup_in_progress = TRUE;
        dapl_cleanup_instance(dapl_instance_ptr);
    }
}


/*
 * to track RDMA WRITE  and RDMA READ Completion
 * RDMA WRITE is initiated by the remote Writer process
 * while RDMA READ is initiated by local Reader process
 */
static int dapl_read_event(struct dapl_device *dev, unsigned long long xfer_len, struct htx_data *stats)
{
    DAT_COUNT   nmore;
    DAT_RETURN  ret = DAT_SUCCESS;
	unsigned long long buf_len, msg_type;

    /* use wait to dequeue */
    ret = dat_evd_wait(dev->h_evd.dto_rcv, /* DTO_TIMEOUT */   DAT_TIMEOUT_INFINITE , 1, &dev->event, &nmore);
    if (ret != DAT_SUCCESS) {
        fprintf(stderr, "Error waiting on h_dto_evd %p: %s\n", dev->h_evd.dto_rcv, dapl_ret_tostr(ret));
    	return (ret);
    }
	if (dev->event.event_number != DAT_DTO_COMPLETION_EVENT) {
		fprintf(stderr, "%d Error unexpected DTO event : %s\n", getpid(), dapl_event_tostr(dev->event.event_number));
        return (DAT_ABORT);
    }
	buf_len = dev->event.event_data.dto_completion_event_data.transfered_length;
	msg_type = dev->event.event_data.dto_completion_event_data.user_cookie.as_64;

	if ((msg_type == MSG_RDMA_WRITE) || (msg_type == MSG_RDMA_READ)) {
		if (buf_len != xfer_len) {
        	return (DAT_ABORT);
		}
	}
	return (ret);
}

static int SendAddrExcToSemServ(struct dapl_device *dev, DAT_RMR_TRIPLET *rbuf, int WriterIdx, struct sockaddr_in *dest, struct htx_data *stats, u_long state)
{

    SOCKET SemServSock;
    struct CoordMsg CMsg;
    char msg_text[4096];
    int rc;
	DAT_RMR_TRIPLET snd_rmr_msg;

	printf("Entry: %s\n", __FUNCTION__);

    SemServSock = SetUpConnect(dest, stats, state);


	/* The first message is sent to block my writer 
	 */ 
    CMsg.msg_type = htonl(DAPL_RMR_XCHG);
   	CMsg.ID.rmr_info.WriterIdx = htons((u_short) 0xffff  & WriterIdx);
    CMsg.ID.rmr_info.local_iov.lmr_context = 0x00;
    CMsg.ID.rmr_info.local_iov.virtual_address = 0xFF;
    CMsg.ID.rmr_info.local_iov.segment_length = 0x00;
 
    rc = StreamWrite(SemServSock, (char *)&CMsg, sizeof(CMsg));
    #ifdef __DEBUG__
        sprintf(msg_text, "%s , ReaderIdx=%d, sending DAPL_RMR_XCHG to SemServ, write_ahead= %d\n", __FUNCTION__, WriterIdx, CMsg.ID.SemID.WriteAhead);
        hxfmsg(stats, HTXERROR(EX_WRITE5,ERRNO), HTX_HE_INFO, msg_text);
    #endif
    if(rc == -1) {
        sprintf(msg_text, "Error : Sending CM_RESET_WRITE to SemServSock - %s \n", STRERROR(errno));
        hxfmsg(stats, HTXERROR(EX_WRITE5,ERRNO), HTX_HE_SOFT_ERROR, msg_text);
		return(rc); 
    }

	memset(&CMsg, 0x00, sizeof(struct CoordMsg)); 
	/*
     *  Setup our msg containing our remote memory info and tell the other side about it
     */
	CMsg.msg_type = htonl(DAPL_RMR_XCHG);
	CMsg.ID.rmr_info.WriterIdx = htons((u_short) 0xffff  & WriterIdx); 
	CMsg.ID.rmr_info.local_iov.lmr_context = htonl(dev->rcv_mr.lmr_ctx); 
	CMsg.ID.rmr_info.local_iov.virtual_address = htonll(dev->rcv_mr.registered_addr);
	CMsg.ID.rmr_info.local_iov.segment_length = htonl(sizeof(DAT_RMR_TRIPLET));
	rc = StreamWrite(SemServSock, (char *)&CMsg, sizeof(CMsg));  
	#ifdef __DEBUG__
		sprintf(msg_text, "%s , ReaderIdx=%d, Sending DAPL_RMR_XCHG, virtual_address = 0x%llx, lmr_context = %d \n", __FUNCTION__, WriterIdx,
					CMsg.ID.rmr_info.local_iov.virtual_address, CMsg.ID.rmr_info.local_iov.lmr_context);
		hxfmsg(stats, HTXERROR(EX_WRITE5,ERRNO), HTX_HE_INFO, msg_text);
	#endif
    if(rc == -1) {
        sprintf(msg_text, "Error : %s: Sending DAPL_RMR_XCHG to SemServSock - %s \n", __FUNCTION__, STRERROR(errno));
        hxfmsg(stats, HTXERROR(EX_WRITE5,ERRNO), HTX_HE_SOFT_ERROR, msg_text);
		return(rc);
    }
	/* Wait for remote end to communicate its TRIPLET */ 
	memset(&CMsg, 0, sizeof(struct CoordMsg));
	printf("Reader[%d] waiting for RMR info from writer\n", getpid());
    rc = StreamRead(SemServSock, (char *)&CMsg, sizeof(struct CoordMsg));
    if(rc == -1) {
        sprintf(msg_text, "Error : %s: Receiving RMR info from writer - %s \n", __FUNCTION__, STRERROR(errno));
        hxfmsg(stats, HTXERROR(EX_WRITE5,ERRNO), HTX_HE_SOFT_ERROR, msg_text);
		return(rc); 
	}
	printf("Reader[%d] received RMR info from writer. msg_type = %d, WriterIdx= %d \n", getpid(), ntohl(CMsg.msg_type), WriterIdx);
	if((WriterIdx != ntohs(CMsg.ID.rmr_info.WriterIdx)) || (ntohl(CMsg.msg_type) != DAPL_RMR_XCHG)) { 
		sprintf(msg_text, "Error : %s: recvd corrupted DAPL_RMR_XCHG from SemServSock, WriterIdx = %d - msg_type=%#llx \n", __FUNCTION__, ntohs(CMsg.ID.rmr_info.WriterIdx), ntohs(CMsg.msg_type)); 
		hxfmsg(stats, HTXERROR(EX_WRITE5,ERRNO), HTX_HE_SOFT_ERROR, msg_text); 
		return(rc); 	
	} 

    /* update reader rbuf info */
	rbuf->virtual_address =  ntohll(CMsg.ID.rmr_info.local_iov.virtual_address);
	rbuf->segment_length = ntohl(CMsg.ID.rmr_info.local_iov.segment_length);
	rbuf->rmr_context = ntohl(CMsg.ID.rmr_info.local_iov.lmr_context);

	#ifdef __DEBUG__ 
		sprintf(msg_text, "%s ,ReaderIdx=%d,Recvd DAPL_RMR_XCHG, virtual_address = %llx, rmr_context = %d \n", __FUNCTION__, WriterIdx,
								rbuf->virtual_address, rbuf->rmr_context);
		hxfmsg(stats, HTXERROR(EX_WRITE5,ERRNO), HTX_HE_INFO, msg_text);
	#endif 
	/* Sent an Ack back to Writer, that we are ready to receive packats */ 
	memset(&CMsg, 0, sizeof(struct CoordMsg)); 
	CMsg.msg_type = htonl(DAPL_RMR_XCHG_ACK); 
	CMsg.ID.rmr_info.WriterIdx = htonl((u_short) 0xffff  &  WriterIdx); 
	CMsg.ID.rmr_info.local_iov.lmr_context = 0x00; 
	CMsg.ID.rmr_info.local_iov.virtual_address =  0xFF; 
	CMsg.ID.rmr_info.local_iov.segment_length  = 0x00; 	
	rc = StreamWrite(SemServSock, (char *)&CMsg, sizeof(CMsg)); 
	if(rc == -1) { 
		sprintf(msg_text, "Error : %s: ACK_ADDRESS to SemServSock - %s \n", __FUNCTION__, STRERROR(errno)); 
		hxfmsg(stats, HTXERROR(EX_WRITE5,ERRNO), HTX_HE_SOFT_ERROR, msg_text); 
		return(rc); 
	}

    closesocket(SemServSock);

	return (0);
}

static void
SendTermToSemServ(int WriteIdx, struct sockaddr_in * dest, struct htx_data * stats, u_long state) {
    SOCKET SemServSock;
    struct CoordMsg CMsg;
    char msg_text[100];
    int rc ;

    SemServSock = SetUpConnect(dest, stats, state);
    CMsg.msg_type = htonl(CM_TERM_WRITE);
    CMsg.ID.SemID.WriteIdx   = htons((u_short) 0xffff & WriteIdx);

    rc = StreamWrite(SemServSock, (char *)&CMsg, sizeof(CMsg));
    sprintf(msg_text, "sending CM_TERM_WRITE to SemServ, rc = %d \n", rc);
    hxfmsg(stats, HTXERROR(EX_WRITE5,ERRNO), HTX_HE_INFO, msg_text);

    if(rc == -1) {
        sprintf(msg_text, "Error : Sending CM_RESET_WRITE to SemServSock - %s \n", STRERROR(errno));
        hxfmsg(stats, HTXERROR(EX_WRITE5,ERRNO), HTX_HE_SOFT_ERROR, msg_text);
    }
    closesocket(SemServSock);

}

