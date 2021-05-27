#ifndef _TAILOR_H
#define _TAILOR_H

#include "arch.h"
#include "event.h"

#define TAILOR_JVMTI_MONITOR_SIGNAL (1)
#define TAILOR_JVMTI_MONITOR_CHANNEL (0)
#define TAILOR_CHANNEL_SIZE (128)

// DESIGN: a thread local pointer points to a thread local buffer mmaped to /tmp/.tailor_jvmti/tid.signal

struct tailor_jvmti_monitor_signal {
    int jhash;
    u64 starttime;
    u64 timestamp;
    u64 duration;
    u64 jaddress;
};

struct tailor_signal {
        u64 timestamp;
        u32 seq;
        u32 type;

        int tid, pid, cgid;
	    union {
	           	struct tailor_jvmti_monitor_signal monitor_signal;
        };
};

void gen_tailor_jvmti_monitor_signal(LockEvent *e);
void delete_tailor_signal_files();

#endif