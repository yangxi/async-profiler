import sys, os, time, datetime

def read_stat_line(stat_file):
    with open(stat_file, 'r') as f:
        return f.readline()
def read_cgroup_tasks(cgrouppath):
    with open(cgrouppath+"/tasks", 'r') as f:
        return f.readlines();

def get_command(procfile):
    with open(procfile,'r') as f:
        return f.readline().strip();
def read_cgroup_cpustat(process):
#   nr_periods 286460
#   nr_throttled 7091
#   throttled_time 232729468083
    ret = {}
    with open(process["cgroup_statpath"], 'r') as f:
        for l in f.readlines():
            kv = l.strip().split(' ')
            ret[kv[0]] = int(kv[1])
    process["cgroup_curr_stat"] = ret;

def swap_cgroup_cpustat(process):
    process["cgroup_last_stat"] = process["cgroup_curr_stat"]
def sync_with_cgroup_period(process):
    read_cgroup_cpustat(process)
    swap_cgroup_cpustat(process)
    while True:
        read_cgroup_cpustat(process)
        if (process["cgroup_curr_stat"]["nr_periods"] != process["cgroup_last_stat"]["nr_periods"]):
            break

def get_uk_time(stat_line):
    stats = stat_line.strip().split(' ')
    utime = int(stats[13]) * 10
    ktime = int(stats[14]) * 10
    ret = [utime + ktime, utime, ktime]
    return ret

def process_thread_stat(process, tickCycle):
    threads = process["threads"]
    for t in threads:
        curr_ku = threads[t]["curr_stat"]["stat"]
        last_ku = threads[t]["last_stat"]["stat"]
        diff_ku = [curr_ku[0] - last_ku[0], curr_ku[1] - last_ku[1], curr_ku[2] - last_ku[2]]
        threads[t]["ticks"][tickCycle] = diff_ku

def read_threads_stat(process):
    process["last_sample"] = time.time() * 1000
    threads = process["threads"]
    # read cgroup stat
#    process["cgroup"]
    #dead_tasks = []
    for t in process["threads"]:
        try:
            if threads[t]["active"]:
                now = time.time() * 1000
                cpu_stat = get_uk_time(read_stat_line(threads[t]["stat_path"]))
                threads[t]["curr_stat"] = {"stamp":now, "stat": cpu_stat}
        except:
            threads[t]["active"] = False;
            if "last_stat" in threads[t]:
                threads[t]["curr_stat"] = {"stamp":now, "stat": threads[t]["last_stat"]["stat"]}
                print("Task %s exits, with %.2f ms \n" % (t, now - threads[t]["last_stat"]["stamp"]))
            else:
                threads[t]["curr_stat"] = {"stamp":now, "stat": [0, 0, 0]}
                print("Task %s exits \n")

            #dead_tasks.append(t);
    #for t in dead_tasks:
    #    process["dead_threads"][t] = threads[t];
    #    threads.pop(t, None)
    process["last_sample_end"] = time.time() * 1000
def swap_thread_stat(process):
    threads = process["threads"]
    for t in process["threads"]:
        threads[t]["last_stat"] = threads[t]['curr_stat']

def report_process(process):
    if process['reporting_mode'] == 'period':
        threads = process["threads"]

#        nr_throttle = process["cgroup_curr_stat"]["nr_throttled"] - process["cgroup_last_stat"]["nr_throttled"]
        #nr_period = process["cgroup_curr_stat"]["nr_periods"] - process["cgroup_last_stat"]["nr_periods"]
        #throttle_time = (process["cgroup_curr_stat"]["throttled_time"] - process["cgroup_last_stat"]["throttled_time"])/1000000.0
        now = time.time()
        this_period_ms = (now - process['start_period']) * 1000
        monitoring_ms = (now - process['start_time']) * 1000
        period_totalTime = 0
        period_totalUser = 0
        period_totalKern = 0
        busy_tick = -1
        busy_totalTime = 0
        busy_totalUser = 0
        busy_totalKern = 0
        l=""
        for i in range(0, process["nrTick"]):
            l += "#######tick %d#######\n" % i;
            l += "====Tasks tid (name): totalTime, user, kernel=====\n"
            totalTime = 0.0
            totalUser = 0.0
            totalKern = 0.0
            for t in process["threads"]:
                    thread_b_tick = threads[t]["ticks"][i]
                    thread_id = threads[t]["tid"]
                    thread_name = threads[t]["name"]
                    if thread_b_tick[0] > 0:
                        l += "%s(%s %s): %.2f, %.2f, %.2f\n" % (thread_id, thread_name, str(threads[t]["active"]),thread_b_tick[0], thread_b_tick[1], thread_b_tick[2])
                        totalTime += thread_b_tick[0]
                        totalUser += thread_b_tick[1]
                        totalKern += thread_b_tick[2]
            period_totalTime += totalTime
            period_totalUser += totalUser
            period_totalKern += totalKern
            if totalTime  > busy_totalTime:
                busy_tick = i
                busy_totalTime = totalTime
                busy_totalUser = totalUser
                busy_totalKern = totalKern
            # for t in process["dead_threads"]:
            #      if len(threads[t]["ticks"]) > i:
            #          thread_b_tick = threads[t]["ticks"][i]
            #          thread_id = threads[t]["tid"]
            #          thread_name = threads[t]["name"]
            #          if thread_b_tick[0] > 0:
            #              l += "%s(%s): %.2f, %.2f, %.2f\n" % (thread_id, thread_name, thread_b_tick[0], thread_b_tick[1], thread_b_tick[2])
            l += "Total %.2f, %.2f, %.2f\n"%(totalTime, totalUser, totalKern)
        p="\n\n\n*********Reporting Period %d*********\n" % (process['periods'])
        p+="Period Latency: %.2f ms, Monitoring Time %.2fms\n" % (this_period_ms, monitoring_ms)
        #p+="Cgroup Throttling: %d (%.2f ms), Cgroup Throttling Periods In This Period: %d\n" % (nr_throttle, throttle_time, nr_period)
        p+="Timestamp %f Date %s Total CPU time %.2f, total user %.2f, total kernel %.2f\n" % (now, time.asctime(time.gmtime(now)), period_totalTime, period_totalUser, period_totalKern)
        p+="Busiest tick in this period %d, total tick CPU time %.2f, user time %.2f, kernel time %.2f" % (busy_tick, busy_totalTime, busy_totalUser, busy_totalKern)
        print(p)
        print(l);
        inactive = []
        for t in process["threads"]:
            if threads[t]["active"] == False:
                print("Task %s deldeted in this round\n" % t)
                inactive.append(t)
        for t in inactive:
            threads.pop(t,None)
def report_busies_tick(process):
    total_ticks = process["threads"]["pid"]["ticks"]
    pid = process["pid"]
    pid_name = process["name"]
    threads = process["threads"]
    busies_tick = 0
    total_time = -1
    for i in range(0, len(total_ticks)):
        this_tick = total_ticks[i]
        this_total_time = this_tick[0]
        if this_total_time > total_time:
            total_time = this_total_time
            busies_tick = i
    # "tick  total:totalu:totalk [tidTotal:tidu:tidk]"
    l = "****The busiest tick: %d\t**********************\n"%(busies_tick)
    total_b_tick = total_ticks[busies_tick]
    l += "====Total pid (name): totalTime(ms), user, kernel===\n"
    l += "%s(%s): %.2f, %.2f, %.2f\n"%(pid, pid_name, total_b_tick[0], total_b_tick[1], total_b_tick[2])
    l += "====Threads tid (name): totalTime, user, kernel=====\n"
    for t in process["tids"]:
        thread_b_tick = threads[t]["ticks"][busies_tick]
        thread_id = threads[t]["tid"]
        thread_name = threads[t]["name"]
        if thread_b_tick[0] > 0:
            l += "%s(%s): %.2f, %.2f, %.2f\n" % (thread_id, thread_name, thread_b_tick[0], thread_b_tick[1], thread_b_tick[2])
    print(l)
def report_throttle_period(process):
    if process['reporting_mode'] == 'period' or process["cgroup_curr_stat"]["nr_throttled"] > process["cgroup_last_stat"]["nr_throttled"]:
        threads = process["threads"]

        nr_throttle = process["cgroup_curr_stat"]["nr_throttled"] - process["cgroup_last_stat"]["nr_throttled"]
        nr_period = process["cgroup_curr_stat"]["nr_periods"] - process["cgroup_last_stat"]["nr_periods"]
        throttle_time = (process["cgroup_curr_stat"]["throttled_time"] - process["cgroup_last_stat"]["throttled_time"])/1000000.0
        now = time.time()
        this_period_ms = (now - process['start_period']) * 1000
        monitoring_ms = (now - process['start_time']) * 1000
        period_totalTime = 0
        period_totalUser = 0
        period_totalKern = 0
        busy_tick = -1
        busy_totalTime = 0
        busy_totalUser = 0
        busy_totalKern = 0
        l=""
        for i in range(0, process["nrTick"]):
            l += "#######tick %d#######\n" % i;
            l += "====Tasks tid (name): totalTime, user, kernel=====\n"
            totalTime = 0.0
            totalUser = 0.0
            totalKern = 0.0
            for t in process["threads"]:
                    thread_b_tick = threads[t]["ticks"][i]
                    thread_id = threads[t]["tid"]
                    thread_name = threads[t]["name"]
                    if thread_b_tick[0] > 0:
                        l += "%s(%s %s): %.2f, %.2f, %.2f\n" % (thread_id, thread_name, str(threads[t]["active"]),thread_b_tick[0], thread_b_tick[1], thread_b_tick[2])
                        totalTime += thread_b_tick[0]
                        totalUser += thread_b_tick[1]
                        totalKern += thread_b_tick[2]
            period_totalTime += totalTime
            period_totalUser += totalUser
            period_totalKern += totalKern
            if totalTime  > busy_totalTime:
                busy_tick = i
                busy_totalTime = totalTime
                busy_totalUser = totalUser
                busy_totalKern = totalKern
            # for t in process["dead_threads"]:
            #      if len(threads[t]["ticks"]) > i:
            #          thread_b_tick = threads[t]["ticks"][i]
            #          thread_id = threads[t]["tid"]
            #          thread_name = threads[t]["name"]
            #          if thread_b_tick[0] > 0:
            #              l += "%s(%s): %.2f, %.2f, %.2f\n" % (thread_id, thread_name, thread_b_tick[0], thread_b_tick[1], thread_b_tick[2])
            l += "Total %.2f, %.2f, %.2f\n"%(totalTime, totalUser, totalKern)
        p="\n\n\n*********Reporting Period %d*********\n" % (process['periods'])
        p+="Period Latency: %.2f ms, Monitoring Time %.2fms\n" % (this_period_ms, monitoring_ms)
        p+="Cgroup Throttling: %d (%.2f ms), Cgroup Throttling Periods In This Period: %d\n" % (nr_throttle, throttle_time, nr_period)
        p+="Total CPU time %.2f, total user %.2f, total kernel %.2f\n" % (period_totalTime, period_totalUser, period_totalKern)
        p+="Busiest tick in this period %d, total tick CPU time %.2f, user time %.2f, kernel time %.2f" % (busy_tick, busy_totalTime, busy_totalUser, busy_totalKern)
        print(p)
        print(l);
        inactive = []
        for t in process["threads"]:
            if threads[t]["active"] == False:
                print("Task %s deldeted in this round\n" % t)
                inactive.append(t)
        for t in inactive:
            threads.pop(t,None)
def report_busies_tick(process):
    total_ticks = process["threads"]["pid"]["ticks"]
    pid = process["pid"]
    pid_name = process["name"]
    threads = process["threads"]
    busies_tick = 0
    total_time = -1
    for i in range(0, len(total_ticks)):
        this_tick = total_ticks[i]
        this_total_time = this_tick[0]
        if this_total_time > total_time:
            total_time = this_total_time
            busies_tick = i
    # "tick  total:totalu:totalk [tidTotal:tidu:tidk]"
    l = "****The busiest tick: %d\t**********************\n"%(busies_tick)
    total_b_tick = total_ticks[busies_tick]
    l += "====Total pid (name): totalTime(ms), user, kernel===\n"
    l += "%s(%s): %.2f, %.2f, %.2f\n"%(pid, pid_name, total_b_tick[0], total_b_tick[1], total_b_tick[2])
    l += "====Threads tid (name): totalTime, user, kernel=====\n"
    for t in process["tids"]:
        thread_b_tick = threads[t]["ticks"][busies_tick]
        thread_id = threads[t]["tid"]
        thread_name = threads[t]["name"]
        if thread_b_tick[0] > 0:
            l += "%s(%s): %.2f, %.2f, %.2f\n" % (thread_id, thread_name, thread_b_tick[0], thread_b_tick[1], thread_b_tick[2])
    print(l)

def observe_process(process, tick, reportTick):
    threads = process["threads"]
    nr_tick = 0
    read_threads_stat(process)
    swap_thread_stat(process)
    now = time.time()
    process['start_time'] = now
    process['periods'] = 1
    process['start_period'] = now
    while True:
        nthTick = nr_tick % reportTick
        next_tick = process["last_sample"] + tick
        now = time.time() * 1000
        if next_tick > now:
#            print("sleep %dMS" % (next_tick - now))
            os.system("sudo perf trace -e 'syscalls:sys_enter_write' -p 25980 -- sleep 1")
#            time.sleep((next_tick - now)/1000.0)
        read_threads_stat(process)
        process_thread_stat(process, nthTick)
        nr_tick += 1
        if (nr_tick % reportTick == 0):
            report_process(process)
            addtasks(process, process["process"])
            read_threads_stat(process)
            process['periods'] += 1
            process['start_period'] = time.time()
        swap_thread_stat(process)
        #report_busies_tick(process)


def observe_cgroup(process, tick, reportTick):
    threads = process["threads"]
    nr_tick = 0
    # sync with cgroup timer
    if process["cgroup_sync"] == True:
        sync_with_cgroup_period(process)
    else:
        read_cgroup_cpustat(process)
        swap_cgroup_cpustat(process)
    read_threads_stat(process)
    swap_thread_stat(process)
    now = time.time()
    process['start_time'] = now
    process['periods'] = 1
    process['start_period'] = now
    while True:
        nthTick = nr_tick % reportTick
        next_tick = process["last_sample"] + tick
        now = time.time() * 1000
        if next_tick > now:
#            print("sleep %dMS" % (next_tick - now))
            time.sleep((next_tick - now)/1000.0)
        read_threads_stat(process)
        process_thread_stat(process, nthTick)
        nr_tick += 1
        if (nr_tick % reportTick == 0):
            # time to analyze cgroup and report throttling
            read_cgroup_cpustat(process)
            report_throttle_period(process)
            for task in read_cgroup_tasks(process["cgrouppath"]):
                addtasks(process, task.strip());
            if process["cgroup_sync"] == True:
                sync_with_cgroup_period(process)
            swap_cgroup_cpustat(process)
            read_threads_stat(process)
            process['periods'] += 1
            process['start_period'] = time.time()
        swap_thread_stat(process)
        #report_busies_tick(process)

def addtasks(process, pid):
    if not os.path.exists("/proc/%s" % pid):
        print("/proc/%s does not exist" % pid)
        return
    threads = process["threads"]
    pid_task_dir="/proc/%s/task" % pid
    comdline = get_command("/proc/%s/cmdline" % pid);
    threads_ids=os.listdir(pid_task_dir)
    with open("/proc/%s/comm" % (pid), 'r') as comm_f:
            process_name = comm_f.readline().rstrip('\n')
            process_stat_path = '/proc/%s/stat' % (pid)
            for i in threads_ids:
                if i in threads:
                    continue;
                try:
                    with open("/proc/%s/task/%s/comm" % (pid, i), 'r') as comm_f:
                        thread_name = comm_f.readline().rstrip('\n')
                        thread_stat_path = '/proc/%s/task/%s/stat' % (pid, i)
                        thread_stat = read_stat_line(thread_stat_path)
                        #print("Add Task %s %s %s %s %s %s" % (pid, i, process_name, thread_name, str(get_uk_time(thread_stat)), comdline))
                        print("Add Task %s %s %s %s %s" % (pid, i, process_name, thread_name, str(get_uk_time(thread_stat))))
                        threads[i] = {"name": thread_name, "active":True, "tid": i, "stat_path": thread_stat_path, "ticks":[]}
                        for j in range(0, process["nrTick"]):
                            threads[i]["ticks"].append([])
                except:
                    print("Can't read thread %s\n" % i);

def observe(tick, reportTick, cgrouppath, reporting_mode,cgroup_sync):
    if cgrouppath.startswith("/sys/fs"):
        process = {"cgroup_statpath":cgrouppath+"/cpu.stat", "cgrouppath":cgrouppath, "threads":{}, "tick":tick, "nrTick":reportTick, "reporting_mode": reporting_mode, 'cgroup_sync': cgroup_sync}
        for task in read_cgroup_tasks(cgrouppath):
            addtasks(process, task.strip())
        observe_cgroup(process, tick, reportTick)
    else:
        process = {"process":cgrouppath, "threads":{}, "tick":tick, "nrTick":reportTick, "reporting_mode": reporting_mode, 'cgroup_sync': False}
        addtasks(process, cgrouppath)
        observe_process(process, tick, reportTick)


    #pid_stat_f = open(pid_stat,'r')
    # for i in range(0, 1000):
    #     now = time.time();
    #     l = pid_stat_f.readline()
    #     then = time.time();
    #     print("%.3f %s" %((then-now)*1000, l))

if __name__ == "__main__":
    usage="""example:
                throttledtask.py /sys/fs/cgroup/cpu/mesos/34005f51-eb6b-407c-9179-0539891c2d2c
                    Sampling the cgroup every 20ms (tick) and every 5 ticks (period) reporting the stat
                throttledtask.py /sys/fs/cgroup/cpu/mesos/34005f51-eb6b-407c-9179-0539891c2d2c 20 5 throttling sync_with_cgroup
                    Sampling the cgroup every 20ms (tick), every 5 ticks (period) reports the stat if there is throttling, and synchronizing with the cgroup throttling period after reporting results.
             throttledtask.py cgroup sampling_tick reporting_period mode [no_cgroup_sync]\n
             cgroup: The cgroup path of the Mesos container, e.g. /sys/fs/cgroup/cpu/mesos/CGROUP_ID
             sampling_tick (ms): the time between two samples
             report_period: the frequency of reporting per-tick per-task resource usage (every N x samplig_tick ms)
             mode:
                "period" [default]: reporting the usage
                "throttling": reporting the usage after detecting the throttling in this cgroup
            no_cgroup_sync: if set, donot sync with the cgroup throttling period after reporting the
             """
    if len(sys.argv) < 2:
        print("Missing the target\nUsage:%s"%(usage))
        exit(1)
    cgroup_path = sys.argv[1]
    tick = 20
    period = 5
    cgroup_sync = True
    reporting_mode = 'period'
    if len(sys.argv) > 2:
        tick = int(sys.argv[2])
    if len(sys.argv) > 3:
        period = int(sys.argv[3])
        if reporting_mode == 'throttling':
            reporting_mode = 'throttling'
    if len(sys.argv) > 4:
        if sys.argv[4] == 'throttling':
            reporting_mode = 'throttling'
    if len(sys.argv) > 5:
        if sys.argv[5] == 'no_cgroup_sync':
            cgroup_sync = False
    observe(tick, period, cgroup_path, reporting_mode, cgroup_sync)


