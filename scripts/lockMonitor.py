import sys
import os
import getopt
import json
import time
import socket
from signal import signal, SIGINT, SIGQUIT
from subprocess import run, Popen, PIPE
import graphyte
from lockstatParser import parseLockStat

verbose = False
jpid = None
def getJavaPid():
    get_jpid = run(['pidof', 'java'], stdout=PIPE, encoding='utf-8')
    if get_jpid.returncode != 0:
        return []
    pids = get_jpid.stdout.strip().split()
    return pids
# we want to report
# hostname.lockstat.totalduration:
# hostname.lockstat.hottesttype.type: java.lang.object
# hostname.lockstat.hottesttype.duration: Xs
# hostname.lockstat.hottesttype.percent: %s
# hostname.lockstat.hottestlock.jhash: ID
# hostname.lockstat.hottestlock.duration: Xs
def getProfileOutput(profile, id):
    if 'Error' in profile:
        return {'Error': profile['Error']}
    ret = {}
    try:
        key = "%s.lockstat.totalcpu" % (id)
        # the unit is in ms
        ret[key] = profile['CPUUsage']['Total'] / 1000.0
        key = "%s.lockstat.totalusercpu" % (id)
        ret[key] = profile['CPUUsage']['User'] / 1000.0
        key = "%s.lockstat.totalsystemcpu" % (id)
        ret[key] = profile['CPUUsage']['System'] / 1000.0
        key = "%s.lockstat.lockduration" % (id)
        ret[key] = profile['TotalDuration'] / (1000 * 1000 * 1000.0)
        hottesttype = profile['Summary'][0]
        key = "%s.lockstat.hottesttype.type" % id
        ret[key] = hottesttype['LockType']
        key = "%s.lockstat.hottesttype.duration" % id
        ret[key] = hottesttype['DurationNS'] / (1000 * 1000 * 1000.0)
        key = "%s.lockstat.hottesttype.percent" % id
        ret[key] = hottesttype['Percentage']
        # hottest lock
        hottestlock = profile['Locks'][0]
        key = "%s.lockstat.hottestlock.jhash" % id
        ret[key] = hottestlock['LockHash']
        key = "%s.lockstat.hottestlock.duration" % id
        ret[key] = hottestlock['DurationNS'] / (1000.0 * 1000 * 1000)
        key = "%s.lockstat.hottestlock.percent" % id
        ret[key] = hottestlock['Percentage']
    except Exception as e:
        return {'Error':str(e)}
    return ret

def sighandler(signal, frame):
    global jpid
    if jpid:
        stop_profiling = os.system("./profiler.sh stop %s > /dev/null" % jpid)
        if stop_profiling != 0:
            print("Signal handler failed to stop the profiler", file=sys.stderr)
    sys.exit(2)

def read_stat_line(stat_file):
    with open(stat_file, 'r') as f:
        return f.readline()
def get_uk_time(stat_line):
    stats = stat_line.strip().split(' ')
    utime = int(stats[13]) * 10
    ktime = int(stats[14]) * 10
    ret = [utime + ktime, utime, ktime]
    return ret
def read_pid_time(pid):
    proc_file = "/proc/%s/stat" % (pid)
    return get_uk_time(read_stat_line(proc_file))

def profileJVM(pid, interval):
    global verbose
    before_uk = read_pid_time(pid)
    p = run(['./profiler.sh', '-d', interval, '-e', 'lock', pid], stdout=PIPE, encoding='utf-8')
    if p.returncode != 0:
        return {'Error': 'Async-profiler failed'}
    if verbose:
        print("Profile output: %s" % (p.stdout))
    after_uk = read_pid_time(pid)
    lockstat = parseLockStat(p.stdout)
    lockstat['CPUUsage'] = {'Total': after_uk[0] - before_uk[0],
        'User': after_uk[1] - before_uk[1],
        'System':after_uk[2] - before_uk[2]}
    return lockstat

def usage():
    print("""usage: lockMonitor.py [-p jvmpid] [-i tag] [-d duration] [-g GRAPHITEIP:PORT] [-h] [-v])""")
    exit(2)
def main(argv):
    global verbose
    global jpid
    search_pid = True
    output = "text"
    id = None
    gip = None
    gport = None
    interval = "60"
    graphyte_sender = None
    # search the Java pid
    # call async profiler to monitor the java pid for 60 minutes
    # parse the output
    # write the metrics to
    try:
        opts, args = getopt.getopt(argv[1:], "hvp:g:d:o:i:", ["help"])
    except getopt.GetoptError as  err:
            # print help information and exit:
            print(str(
                err))  # will print something like "option -a not recognized"
            usage()
            sys.exit(2)
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
        elif o in ("-p"):
            search_pid = False
            jpid = a
        elif o in ("-d"):
            interval = a
        elif o in ("-o"):
            output = a
        elif o in ("-i"):
            id = a
        elif o in ("-g"):
            output = "graphite"
            if len(a.split(":")) != 2:
                print("The Graphite parameter is not in right format (IP:PORT)\n")
                usage()
                exit(2)
            gip = a.split(":")[0]
            gport = int(a.split(":")[1])
            graphyte.init(gip, port=gport)

    if id == None:
        id = socket.gethostname()
    if verbose:
        print("Monitor pid:%s at interval %ss with output format %s\n" % (jpid, interval, output))
    signal(SIGINT, sighandler)
    while True:
        if search_pid:
            pids = getJavaPid()
            if len(pids) == 0:
                print("There is no JVM for profiling, exit!\n", file=sys.stderr)
                exit(1)
            elif len(pids) > 1:
                print("There are more than one JVMs, please use the -p parameter", file=sys.stderr)
                print(usage())
                exit(1)
            jpid = pids[0]
        startTime = int(time.time())
        profile = profileJVM(jpid, interval)
        if 'Error' in profile:
            print("Error in profiling:%s" % profile['Error'])
            exit(1)
        if output == "json":
            print(json.dumps(profile, indent=4))
        elif output == "text":
            outo = getProfileOutput(profile, id)
            print("\nstarttime:  %s" % time.asctime(time.gmtime(startTime)))
            print("duration: %s" % interval)
            print("startstamp: %d" % startTime)
            if 'Error' in outo:
                print("error: %s" % outo['Error'])
            else:
                for k in outo.keys():
                    print("%s: %s" % (k, outo[k]))
        elif output == "graphite":
            outo = getProfileOutput(profile, id)
            if 'Error' in outo:
                print("Error: %s" % outo['Error'], file=sys.stderr)
                continue
            for k in outo.keys():
                if not isinstance(outo[k], str):
                    graphyte.send(k, outo[k], timestamp=startTime)
                    if verbose:
                        print("Send %s : %s at timestamp %d to graphyte" % (k, outo[k], startTime))
        else:
            print("Unknow output format:%s" % output)
        # try to search
if __name__ == '__main__':
    exit(main(sys.argv))
