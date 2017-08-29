/* IBM_PROLOG_BEGIN_TAG */
/*
 * Copyright 2003,2016 IBM International Business Machines Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "nest_framework.h"
#ifdef __HTX_LINUX__
#define dcbf(x) __asm ("dcbf 0,%0\n\t": : "r" (x))
#else
#pragma mc_func dcbf    { "7C0018AC" }          /* dcbf r3 */
#endif
extern struct mem_exer_info mem_g;

int mem_operation_write_dword(int num_operations,void *seg_address,void *pattern_ptr,int trap_flag,void *seg,void *stanza,void *pattern_sz_ptr,void* seed){
    int i=0,rc=0;
    unsigned long *rw_ptr = (unsigned long *)seg_address;
    for (i=0; i < num_operations; i++) {
        *rw_ptr = *(unsigned long *)pattern_ptr;
        rw_ptr = rw_ptr + 1;
    }
    rc = 0;
    return (rc);

}

int mem_operation_write_word(int num_operations,void *seg_address,void *pattern_ptr,int trap_flag,void *seg,void *stanza,void *pattern_sz_ptr,void* seed){
    int i=0,rc=0;
    unsigned int *rw_ptr = (unsigned int *)seg_address;
    for (i=0; i < num_operations; i++) {
        *rw_ptr = *(unsigned int*)pattern_ptr;
        rw_ptr = rw_ptr + 1;
    }
    rc = 0;
    return (rc);

}
int mem_operation_write_byte(int num_operations,void *seg_address,void *pattern_ptr,int trap_flag,void *seg,void *stanza,void *pattern_sz_ptr,void* seed){
    int i=0,rc=0;
    unsigned char *rw_ptr = (unsigned char *)seg_address;
    for (i=0; i < num_operations; i++) {
        *rw_ptr = *(unsigned char*)pattern_ptr;
        rw_ptr = rw_ptr + 1;
    }
    rc = 0;
    return (rc);

}
int mem_operation_read_dword(int num_operations,void *seg_address,void *pattern_ptr,int trap_flag,void *seg,void *stanza,void *pattern_sz_ptr,void* seed){
    int i=0,rc=0;
    volatile unsigned long *rw_ptr = (unsigned long *)seg_address;
    unsigned long consume_data=0;
    unsigned long read_data;
    for (i=0; i < num_operations; i++) {
        read_data = *(unsigned long *)rw_ptr;
        consume_data += (read_data+i);/*to avoide compiler optimization*/
        rw_ptr = rw_ptr + 1;
    }
    mem_g.dummy_read_data = consume_data;/*to avoide compiler optimization*/
    rc = 0;
    return (rc);
}

int mem_operation_read_word(int num_operations,void *seg_address,void *pattern_ptr,int trap_flag,void *seg,void *stanza,void *pattern_sz_ptr,void* seed){
    int i=0,rc=0;
    volatile unsigned int *rw_ptr = (unsigned int *)seg_address;
    unsigned long consume_data=0;
    unsigned int read_data;
    for (i=0; i < num_operations; i++) {
        read_data = *(unsigned int *)rw_ptr;
        consume_data += (read_data+i);/*to avoide compiler optimization*/
        rw_ptr = rw_ptr + 1; 
    }
    mem_g.dummy_read_data = consume_data;/*to avoide compiler optimization*/
    rc = 0;
    return (rc);
}
int mem_operation_read_byte(int num_operations,void *seg_address,void *pattern_ptr,int trap_flag,void *seg,void *stanza,void *pattern_sz_ptr,void* seed){
    int i=0,rc=0;
    volatile unsigned char *rw_ptr = (unsigned char *)seg_address;
    unsigned long consume_data=0;
    unsigned char read_data;
    for (i=0; i < num_operations; i++) {
        read_data = *(unsigned char *)rw_ptr;
        consume_data += (read_data+i);/*to avoide compiler optimization*/
        rw_ptr = rw_ptr + 1; 
    }
    mem_g.dummy_read_data = consume_data;/*to avoide compiler optimization*/
    rc = 0;
    return (rc);
}
int mem_operation_comp_dword(int num_operations,void *seg_address,void *pattern_ptr,int trap_flag,void *seg,void *stanza,void *pattern_sz_ptr,void* seed){
    volatile unsigned long *ptr = (unsigned long *)seg_address, *loc_pattern_ptr = (unsigned long *)pattern_ptr;
    unsigned long temp_val1,temp_val2;
    int i=0,rc=0;
    for (i=0; i < num_operations; i++) {
        if( (temp_val1 = *ptr) != (temp_val2 = *loc_pattern_ptr) ) {
            if(trap_flag){
                #ifndef __HTX_LINUX__
                trap(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)ptr,(unsigned long)seg,(unsigned long)stanza);
                #else
                do_trap_htx64(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)ptr,(unsigned long)seg,(unsigned long)stanza,0);
                #endif
            }
            displaym(HTX_HE_SOFT_ERROR,DBG_MUST_PRINT,"[%d]%s:1st read_comp failure,expected data = 0x%lx\tactual data = 0x%lx\n",__LINE__,__FUNCTION__,
                    temp_val2,temp_val1);
            rc = i+1;
            break;
        }
        ptr = ptr + 1;
        rc = 0;
    } 
    return rc;
}
int mem_operation_comp_word(int num_operations,void *seg_address,void *pattern_ptr,int trap_flag,void *seg,void *stanza,void *pattern_sz_ptr,void* seed){
    volatile unsigned int *ptr = (unsigned int *)seg_address, *loc_pattern_ptr = (unsigned int *)pattern_ptr;
    unsigned int temp_val1,temp_val2;
    int i=0,rc=0;
    for (i=0; i < num_operations; i++) {
        if( (temp_val1 = *ptr) != (temp_val2 = *loc_pattern_ptr) ) {
            if(trap_flag){
                #ifndef __HTX_LINUX__
                trap(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)ptr,(unsigned long)seg,(unsigned long)stanza);
                #else
                do_trap_htx64(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)ptr,(unsigned long)seg,(unsigned long)stanza,0);
                #endif
            }
            displaym(HTX_HE_SOFT_ERROR,DBG_MUST_PRINT,"[%d]%s:1st read_comp failure,expected data = 0x%x\tactual data = 0x%x\n",__LINE__,__FUNCTION__,
                    temp_val2,temp_val1);
            rc = i+1;
            break;
        }
        ptr = ptr + 1;
        rc = 0;
    } 
    return rc;
}
int mem_operation_comp_byte(int num_operations,void *seg_address,void *pattern_ptr,int trap_flag,void *seg,void *stanza,void *pattern_sz_ptr,void* seed){
    volatile unsigned char *ptr = (unsigned char *)seg_address, *loc_pattern_ptr = (unsigned char *)pattern_ptr;
    unsigned char temp_val1,temp_val2;
    int i=0,rc=0;
    for (i=0; i < num_operations; i++) {
        if( (temp_val1 = *ptr) != (temp_val2 = *loc_pattern_ptr) ) {
            if(trap_flag){
                #ifndef __HTX_LINUX__
                trap(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)ptr,(unsigned long)seg,(unsigned long)stanza);
                #else
                do_trap_htx64(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)ptr,(unsigned long)seg,(unsigned long)stanza,0);
                #endif
            }
            displaym(HTX_HE_SOFT_ERROR,DBG_MUST_PRINT,"[%d]%s:1st read_comp failure,expected data = 0x%x\tactual data = 0x%x\n",__LINE__,__FUNCTION__,
                    temp_val2,temp_val1);
            rc = i+1;
            break;
        }
        ptr = ptr + 1;
        rc = 0;
    } 
    return rc;
}

int mem_operation_write_addr(int num_operations,void *seg_address,void *pattern_ptr,int trap_flag,void *seg,void *stanza,void *pattern_sz_ptr,void* seed){
    int i=0,rc=0;
    unsigned long address = *(unsigned long *)seg_address;
    unsigned long *rw_ptr = (unsigned long *)seg_address;
    for (i=0; i < num_operations; i++) {
        *rw_ptr = address;
        rw_ptr = rw_ptr + 1;
        address = (unsigned long)rw_ptr;
    }
    rc = 0;
    return (rc);
}

int mem_operation_comp_addr(int num_operations,void *seg_address,void *pattern_ptr,int trap_flag,void *seg,void *stanza,void *pattern_sz_ptr,void* seed){
    int i=0,rc=0;
    unsigned long *ptr = (unsigned long *)seg_address ;
    unsigned long address = *(unsigned long *)seg_address;
    unsigned long temp_val;
    for (i=0; i < num_operations; i++) {
        if( (temp_val = *ptr) != address) {
            if(trap_flag){
                #ifndef __HTX_LINUX__
                trap(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)ptr,(unsigned long)seg,(unsigned long)stanza);
                #else
                do_trap_htx64(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)ptr,(unsigned long)seg,(unsigned long)stanza,0);
                #endif
            }
            displaym(HTX_HE_SOFT_ERROR,DBG_MUST_PRINT,"[%d]%s:1st read_comp failure,expected data = 0x%lx\tactual data = 0x%lx\n",__LINE__,__FUNCTION__,
                    address,temp_val);
            rc = i+1;
            break;
        }
        ptr = ptr + 1;
        address = (unsigned long)ptr;
        rc = 0;
    }
    return rc;
}
int  mem_operation_rim_dword(int num_operations,void *seg_address,void *pattern_ptr,int trap_flag,void *seg,void *stanza,void *pattern_sz_ptr,void* seed){
    int i=0,rc=0;
    unsigned long *w_ptr = (unsigned long *)seg_address;
    unsigned long read_data;
    unsigned long temp_val;
    for (i=0; i < num_operations; i++) {
        *w_ptr = *(unsigned long *)pattern_ptr;
        read_data = *(unsigned long *)w_ptr;
        dcbf((volatile unsigned long*)w_ptr);
        read_data = *(unsigned long *)w_ptr;
        if(read_data != (temp_val = *(unsigned long *)pattern_ptr)) {
            if(trap_flag){
                #ifndef __HTX_LINUX__
                trap(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)w_ptr,(unsigned long)seg,(unsigned long)stanza);
                #else
                do_trap_htx64(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)w_ptr,(unsigned long)seg,(unsigned long)stanza,0);
                #endif
            }
            displaym(HTX_HE_SOFT_ERROR,DBG_MUST_PRINT,"[%d]%s:1st read_comp failure,expected data = 0x%lx\tactual data = 0x%lx\n",__LINE__,__FUNCTION__,
                temp_val,read_data);
            rc = i+1;
            break;
        }
        w_ptr = w_ptr + 1;
        rc = 0;
    }
    return rc;
}
int  mem_operation_rim_word(int num_operations,void *seg_address,void *pattern_ptr,int trap_flag,void *seg,void *stanza,void *pattern_sz_ptr,void* seed){
    int i=0,rc=0;
    unsigned int *w_ptr = (unsigned int*)seg_address;
    unsigned int read_data;
    unsigned int temp_val;
    for (i=0; i < num_operations; i++) {
        *w_ptr = *(unsigned int*)pattern_ptr;
        read_data = *(unsigned int*)w_ptr;
        dcbf((volatile unsigned int*)w_ptr);
        read_data = *(unsigned int*)w_ptr;
        if(read_data != (temp_val = *(unsigned int*)pattern_ptr)) {
            if(trap_flag){
                #ifndef __HTX_LINUX__
                trap(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)w_ptr,(unsigned long)seg,(unsigned long)stanza);
                #else
                do_trap_htx64(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)w_ptr,(unsigned long)seg,(unsigned long)stanza,0);
                #endif
            }
            displaym(HTX_HE_SOFT_ERROR,DBG_MUST_PRINT,"[%d]%s:1st read_comp failure,expected data = 0x%x\tactual data = 0x%x\n",__LINE__,__FUNCTION__,
                temp_val,read_data);

            rc = i+1;
            break;
        }
        w_ptr = w_ptr + 1;
        rc = 0;
    }
    return rc;
}

int  mem_operation_rim_byte(int num_operations,void *seg_address,void *pattern_ptr,int trap_flag,void *seg,void *stanza,void *pattern_sz_ptr,void* seed){
    int i=0,rc=0;
    char *w_ptr = (char*)seg_address;
    char read_data;
    char temp_val;
    for (i=0; i < num_operations; i++) {
        *w_ptr = *(char*)pattern_ptr;
        read_data = *(char*)w_ptr;
        dcbf((volatile char*)w_ptr);
        read_data = *(char*)w_ptr;
        if(read_data != (temp_val = *(char*)pattern_ptr)) {
            if(trap_flag){
                #ifndef __HTX_LINUX__
                trap(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)w_ptr,(unsigned long)seg,(unsigned long)stanza);
                #else
                do_trap_htx64(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)w_ptr,(unsigned long)seg,(unsigned long)stanza,0);
                #endif
            }
            displaym(HTX_HE_SOFT_ERROR,DBG_MUST_PRINT,"[%d]%s:1st read_comp failure,expected data = 0x%x\tactual data = 0x%x\n",__LINE__,__FUNCTION__,
                temp_val,read_data);
            rc = i+1;
            break;
        }
        w_ptr = w_ptr + 1;
        rc = 0;
    }
    return rc;
}

int mem_operation_write_addr_comp(int num_operations,void *seg_address,void *pattern_ptr,int trap_flag,void *seg,void *stanza,void *pattern_sz_ptr,void* seed){
    int i=0,rc=0;
    unsigned long *rw_ptr = (unsigned long *)seg_address;
    unsigned long address = *(unsigned long *)seg_address;
    unsigned long read_dw_data;
    for (i=0;i<num_operations;i++){
        *rw_ptr = address;
        dcbf((volatile unsigned long*)rw_ptr);
        read_dw_data = *(unsigned long *)rw_ptr;
        if(read_dw_data != address) {
            if(trap_flag){
                #ifndef __HTX_LINUX__
                trap(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)rw_ptr,(unsigned long)seg,(unsigned long)stanza);
                #else
                do_trap_htx64(0xBEEFDEAD,i,(unsigned long)seg_address,(unsigned long)pattern_ptr,(unsigned long)rw_ptr,(unsigned long)seg,(unsigned long)stanza,0);
                #endif
            }
            displaym(HTX_HE_SOFT_ERROR,DBG_MUST_PRINT,"[%d]%s:1st read_comp failure,expected data = 0x%lx\tactual data = 0x%lx\n",__LINE__,__FUNCTION__,
                address,read_dw_data);
            rc = i+1;
            break;
        }
        rw_ptr  = rw_ptr + 1;
        address = (unsigned long)rw_ptr;
        rc = 0;
    }
    return rc;
}


