#include "arch.h"
#include "event.h"
#include "tailor.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <sys/un.h>
#include <sys/syscall.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>


thread_local u8* tailor_channel_page = nullptr;
thread_local int tailor_error = 0;
thread_local int my_tid = -1;
thread_local int my_pid = -1;
#define MAX_PATH 1024
#define TMP_PATH (MAX_PATH - 64)
// one page
#define TAILOR_CHANNEL_PAGE_SIZE (4096)
#define TAILOR_CHANNEL_PAGE_MODE (S_IRWXU | S_IRWXG | S_IRWXO)

u8* init_tailor_channel_page() {
    if (tailor_channel_page != nullptr)
        return tailor_channel_page;
    // create /tmp/.tailor_jvmti/tid.signal
    int tid = OS::threadId();
    int pid = OS::processId();
    char tmp_path[TMP_PATH] = {0};
    snprintf(tmp_path, sizeof(tmp_path), "/tmp/tailor_jvmti_%d_%d.signal", pid, tid);
    struct stat fstat;
    int fd = -1;
    if (stat(tmp_path, &fstat) != -1 && fstat.st_size == TAILOR_CHANNEL_PAGE_SIZE) {
        fd = open(tmp_path, O_RDWR);
        if (fd == -1) {
            fprintf(stderr, "Failed to open the tailor channel page file for task %d\n", tid);
            return nullptr;
        }
    } else {
        fd = open(tmp_path, O_RDWR | O_CREAT | O_TRUNC, TAILOR_CHANNEL_PAGE_MODE);
        // fd = creat(tmp_path, TAILOR_CHANNEL_PAGE_MODE);
        if (fd == -1) {
            fprintf(stderr, "Failed to open the tailor channel page file for task %d\n", tid);
            return nullptr;
        }
        char *zeropage = (char *)calloc(1, TAILOR_CHANNEL_PAGE_SIZE);
        if (zeropage == nullptr) {
            fprintf(stderr, "Failed to calloc the zero page\n");
            return nullptr;
        }
        int nr_wrote = write(fd, zeropage, TAILOR_CHANNEL_PAGE_SIZE);
        if (nr_wrote != TAILOR_CHANNEL_PAGE_SIZE) {
            fprintf(stderr, "Failed to write the whole tailor channel page (%d) for task %d ", nr_wrote, tid);
            return nullptr;
        }
        lseek(fd, 0, SEEK_SET);
    }
    char * signal_buf = (char *)mmap(0, TAILOR_CHANNEL_PAGE_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (signal_buf == MAP_FAILED){
	    fprintf(stderr, "Failed to mmap the tailor channel page for task %d", tid);
        return nullptr;
    }
    memset(signal_buf, 0, TAILOR_CHANNEL_PAGE_SIZE);
    tailor_channel_page = (u8 *) signal_buf;
    my_pid = pid;
    my_tid = tid;
    return tailor_channel_page;
}

void delete_tailor_signal_files() {
    struct dirent * de;
    DIR *dr = opendir("/tmp");
    char tailor_prefix[128] = {0};
    char tailor_file[TMP_PATH] = {0};
    int pid = OS::processId();
    snprintf(tailor_prefix, sizeof(tailor_prefix), "tailor_jvmti_%d_", pid);
    if (dr == NULL) {
        fprintf(stderr, "Failed to open the /tmp directory for deleting tailor signal files.\n");
        return;
    }
    while ((de = readdir(dr)) != NULL) {
        if (strncmp(tailor_prefix, de->d_name, strlen(tailor_prefix)) == 0) {
            snprintf(tailor_file, sizeof(tailor_file), "/tmp/%s", de->d_name);
            if (remove(tailor_file) == -1) {
                fprintf(stderr, "Failed to remove the Tailor signal file %s\n", tailor_file);
            }
        }
    }
}

void gen_tailor_jvmti_monitor_signal(LockEvent *e) {
    if (tailor_error != 0)
        return;
    if (tailor_channel_page == nullptr) {
        if (init_tailor_channel_page() == nullptr) {
            tailor_error = 1;
            return;
        }
    }
    struct tailor_signal * s = (struct tailor_signal *) (tailor_channel_page + TAILOR_CHANNEL_SIZE * TAILOR_JVMTI_MONITOR_CHANNEL);
    s->tid = my_tid;
    s->pid = my_pid;
    s->type = TAILOR_JVMTI_MONITOR_SIGNAL;
    s->monitor_signal.starttime = e->_start_time;
    s->monitor_signal.duration = e->_end_time - e->_start_time;
    s->monitor_signal.jaddress = e->_address;
    s->monitor_signal.jhash = e->_jhash;
    s->monitor_signal.timestamp = e->_enter_stamp;
    s->seq += 1;
    s->timestamp = rdtsc();
    // timestamp
}