
import re

# #        NAME                                                              READY   STATUS                 RESTARTS   AGE     IP              NODE                                                  NOMINATED NODE   READINESS GATES
# action-b6967f688-22bdl                                            16/16   Running                16         3h56m   10.40.160.198   gke-primary-gke-action-preempt-202105-0ca50138-8flv   <none>           <none>
def parse_output(lines, index=None, indexFetcher=None):
    # line 0 is the column index
    ret = {}
    colnames = lines[0].strip().split()
    for l in range(1, len(lines)):
        o = {}
        t = lines[l].strip().split()
        for i in range(0, len(colnames)):
            if i >= len(t):
                continue
            o[colnames[i]] = t[i]
        if index and index in o:
            ret[o[index]] = o
        elif indexFetcher:
            new_index = indexFetcher(o)
            if new_index != None:
                ret[new_index] = o
    return ret

def process_fd_line(fdl):
    m = re.match(r'\d+', fdl["FD"])
    t = fdl["TYPE"]
    n = fdl["NAME"]
    if m == None:
        return m
    index = int(m.group())
    fdl["index"] = index
    if t == 'IPv4':
        #10.238.76.251:9092->10.40.157.20:51142 (ESTABLISHED)
        if '->' in n:
            ips = n.split('->')
            from_ip, from_port = ips[0].split(':')
            to_ip, to_port = ips[1].split(':')
            fdl['IP_FROM'] = from_ip
            fdl['PORT_FROM'] = from_port
            fdl['IP_TO'] = to_ip
            fdl['PORT_TO'] = to_port
    # check Kafka
    return index

def load_fds(fname):
    fds = {}
    with open(fname, 'r') as f:
        fds = parse_output(f.readlines(), indexFetcher = process_fd_line)
    return fds

def load_pods(fname):
    pods = {}
    with open(fname, 'r') as f:
        pods = parse_output(f.readlines(), "IP")
    return pods

# class FDs(object):
#     def __init__(self, fdlines):
#         self.fds={}
#         self.fdlines = fdlines
#         self.parse()
#     def parse(self):
# class Pods(object):
#     def __init__(self, podwide):
#         # indexed by the pod IP
#         self.pods={}
#         self.podlines = podlines
#         self.parse()
# #        NAME                                                              READY   STATUS                 RESTARTS   AGE     IP              NODE                                                  NOMINATED NODE   READINESS GATES
# #action-b6967f688-22bdl                                            16/16   Running                16         3h56m   10.40.160.198   gke-primary-gke-action-preempt-202105-0ca50138-8flv   <none>           <none>
# #action-b6967f688-2422n                                            16/16   Running                42         4h      10.41.137.10    gke-primary-gke-action-preempt-202105-81fc5e62-kb1t   <none>           <none>
# #action-b6967f688-24jzb                                            16/16   Running                45         4h1m    10.40.20.140    gke-primary-gke-action-preempt-202105-81fc5e62-l4m1   <none>           <none>
# #action-b6967f688-24m6l                                            16/16   Running                30         3h59m   10.40.108.133   gke-primary-gke-action-preempt-202105-81fc5e62-jppc   <none>           <none>
# #action-b6967f688-259pt                                            16/16   Running                44         3h59m   10.40.110.205   gke-primary-gke-action-preempt-202105-81fc5e62-3gdx   <none>           <none>
# #action-b6967f688-25nj8                                            16/16   Running                44         4h      10.40.209.205   gke-primary-gke-action-preempt-202105-aaf0bf0f-j17w   <none>           <none>
# #action-b6967f688-25ntg                                            16/16   Running                42         4h      10.41.139.162   gke-primary-gke-action-n2d-20210504-2655123a-xz52     <none>           <none>
# #action-b6967f688-25x7l                                            16/16   Running                42         4h1m    10.41.175.212   gke-primary-gke-action-20210503-8f5731e8-klfb         <none>           <none>
# #action-b6967f688-2625v                                            16/16   Running                58         4h1m    10.40.48.206    gke-primary-gke-action-preempt-202105-81fc5e62-fcgd   <none>           <none>
# #action-b6967f688-26ftq                                            16/16   Running                47         4h1m    10.41.175.84    gke-primary-gke-action-n2d-20210504-1c1b6ee6-h8nz     <none>           <none>
#     def parse(self):
#         keys = self.podlines[0].strip().split(' ')
