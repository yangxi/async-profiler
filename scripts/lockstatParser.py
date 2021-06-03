import os
import re
import sys

"""
Parse the Async lock tracing output.
Input Format:
# --- Execution profile ---
Total samples       : 277404
skipped             : 7 (0.00%)

--- 21623068371 ns (12.33%), 19400 samples
  [ 0] java.lang.Object
  [ 1] kafka.log.Log.$anonfun$append$2
  [ 2] kafka.log.Log.append
  [ 3] kafka.log.Log.appendAsLeader
  [ 4] kafka.cluster.Partition.$anonfun$appendRecordsToLeader$1
  [ 5] kafka.cluster.Partition.appendRecordsToLeader
  [ 6] kafka.server.ReplicaManager.$anonfun$appendToLocalLog$4
  [ 7] kafka.server.ReplicaManager$$Lambda$1050.169244457.apply
  [ 8] scala.collection.StrictOptimizedMapOps.map
  [ 9] scala.collection.StrictOptimizedMapOps.map$
  [10] scala.collection.mutable.HashMap.map
  [11] kafka.server.ReplicaManager.appendToLocalLog
  [12] kafka.server.ReplicaManager.appendRecords
  [13] kafka.server.KafkaApis.handleProduceRequest
  [14] kafka.server.KafkaApis.handle
  [15] kafka.server.KafkaRequestHandler.run
  [16] java.lang.Thread.run
  [17] [jhash=474685454]

          ns  percent  samples  top
  ----------  -------  -------  ---
171290812952   97.65%   249382  java.lang.Object
  1796442340    1.02%    20014  kafka.server.FetchSessionCache
  1501598357    0.86%     4975  kafka.utils.timer.TimerTaskList
   457729178    0.26%     1965  java.util.concurrent.ConcurrentHashMap$Node
   170838685    0.10%        8  java.lang.ref.Reference$Lock
   111342416    0.06%      458  org.apache.kafka.common.metrics.Sensor
    57349191    0.03%      264  java.lang.Class
    24634498    0.01%      330  sun.nio.ch.NativeThreadSet
       58329    0.00%        1  kafka.server.DelayedFetch

Return
{ Samples: Val, Skipped: Value,
 Locks:[{LockHash:Val, DurationNS:val, Percentage:val, Samples:Val, Traces:[]}],
 Summary:[{LockType:Str, DurationNS:val, Percentage:val, Samples:val}]
}
"""
def parseLockStat(lockstat):
    t = lockstat.split('\n\n')
    if len(t) < 2:
        print("Failed to parse lockstat %s" % lockstat, file=sys.stderr)
        return {'Error': 'Failed to parse the lockstat'}
    ret = parseHead(t[0])
    if 'Error' in ret:
        return ret
    # locks
    if ret['Samples'] == 0:
        ret['Locks'] = []
        ret['Summary'] = []
        return ret
    locks = []
    for ls in t[1:-1]:
        locks.append(parseLock(ls))
    ret['Locks'] = locks
    summaryState = t[-1]
    summaryLines = t[-1].strip().split('\n')
    summary = []
    totalDuration = 0
    if len(summaryLines) >= 3:
        for sl in summaryLines[2:]:
            pl = parseSummaryLine(sl)
            summary.append(parseSummaryLine(sl))
            totalDuration += pl["DurationNS"]
    ret['Summary'] = summary
    ret['TotalDuration'] = totalDuration
    return ret

def parseHead(headstat):
    lines = headstat.split('\n')
    skipped = 0
    if len(lines) < 2:
        return {'Error': 'Size of the header is not 3 lines.'}
    if lines[0] != '--- Execution profile ---':
        return {'Error': 'Header starts with wrong pattern.'}
    try:
        samples = int(lines[1].split(':')[1].strip())
        if len(lines) > 2:
            skipped = int(lines[2].split(':')[1].strip().split(' ')[0].strip())
    except Exception as e:
        return {'Error': str(e)}
    return {'Samples':samples, 'Skipped':skipped}

def parseLock(lockstat):
    lines = lockstat.split('\n')
    if len(lines) < 3:
        return {'Error':'Unrecognizable lockstat'}
    headerline = lines[0]
    hashline = lines[-1]
    re_header = re.compile(r'^--- ([0-9]+) ns \(([0-9]+\.[0-9]+)\%\), ([0-9]+) samples')
    m_header = re_header.match(headerline)
    re_jhash = re.compile(r'\[jhash=([0-9]+)\]')
    m_jhash = re_jhash.search(hashline)
    if m_header == None or m_jhash == None:
        return {'Error':"Unrecognizable lockstat"}
    ret = {}
    lines = lockstat.split('\n')
    try:
        ret['DurationNS'] = int(m_header.group(1))
        ret['Percentage'] = float(m_header.group(2))
        ret['Samples'] = int(m_header.group(3))
        ret['LockType'] = lines[1].strip().split(' ')[-1]
        ret['LockHash'] = m_jhash.group(1)
        ret['Traces'] = lines[1:]
    except Exception as e:
        return {"Error": str(e)}
    return ret

def parseSummaryLine(line):
    ret = {}
    t = line.strip().split()
    if len(t) != 4:
        return {"Error": "Unrecognizable lock summary"}
    try:
        ret['LockType'] = t[3]
        ret['DurationNS'] = int(t[0])
        ret['Samples'] = int(t[2])
        ret['Percentage'] = float(t[1][0:-1])
    except Exception as e:
        return {"Error": str(e)}
    return ret
