#!/usr/bin/env python
import commands, subprocess, os, sys, signal, time, re, itertools, pickle, telnetlib
#from vmstat import vmstat

# debug ------
verbose = True
stopflag = False

# help functions -----
echo_list = [sys.stdout]
log_list = []
def echo(s):
    for f in echo_list:
        try:
            f.write(str(s) + '\n')
            f.flush()
        except:
            pass

def log(s):
    for f in log_list:
        try:
            f.write(str(s) + '\n')
            f.flush()
        except:
            pass

def run_command(cmd):
    if verbose:
        echo('Run: %s' % cmd)
    (rc, out) = commands.getstatusoutput(cmd)
    if rc:
        echo(out)
        raise Exception('Failed to run %s' % cmd)
    return rc,out

def run_background(cmd):
    if verbose:
        echo('Run: %s' % cmd)
    out = os.system(cmd + " &")
    if out < 0:
        echo(out)
        raise Exception('Failed to run %s' % cmd)
    return out

def run_check(cmd):
    if verbose:
        echo('Run: %s' % cmd)
    (rc, out) = commands.getstatusoutput(cmd)
    if rc:
        echo(out)
        #raise Exception('Failed to run %s' % cmd)
    return rc,out

def run_timed_command(cmd):
    t0 = time.time()
    rc,out = run_command(cmd)
    t1 = time.time()
    elapsed = t1 - t0
    if verbose:
        echo('Elapsed: %5.3f' % (elapsed))
    return elapsed

def ctrlC_handler(signum, frame):
    stopflag = True

def get_sched_stat():
    HOST = "localhost"
    PORT = "11211"
    tn = telnetlib.Telnet(HOST, PORT)
    tn.write("# stat\n")
    out = tn.read_some()
    tn.write("quit")

    #echo(out)
    return out

def my_sum(lst):
    total = 0
    cnt = 0
    for i in lst:
        if not i.isdigit():
            continue

        if cnt != 0:
            total += int(i)
        cnt += 1
    return total

def kill_mcd(host):
    try:
        (rc, out) = run_check('ssh ubuntu@%s "sudo pkill memcached"' % host)
    except:
        pass
    echo(out)

def ex_mcd(host, mem):
    kill_mcd(host)
    try:
        (rc, out) = run_command('ssh ubuntu@%s "memcached -m %s < /dev/null >& /dev/null &"' % (host, mem))
    except:
        pass
    echo(out)

def add_node(node, mem):
    ex_mcd(node, mem)
    HOST = "localhost"
    PORT = "11211"
    try:
        tn = telnetlib.Telnet(HOST, PORT)
        tn.write("# add " + node + "\n")
        out = tn.read_some()
        tn.write("quit")
    except:
        pass
    
    #echo(out)
    return out

def del_node(node):
    HOST = "localhost"
    PORT = "11211"
    try:
        tn = telnetlib.Telnet(HOST, PORT)
        tn.write("# remove " + node + "\n")
        out = tn.read_some()
        tn.write("quit")
    except:
        pass
    
    #echo(out)
    return out

# command functions ------

# main function ------
if __name__ == "__main__":
    import argparse

    # arguments ---
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output-file', dest='output_file', \
                        help="additional output file", \
                        action="store",
                        default = None
                        )

    # parameters 
    parser.add_argument('-i', '--init-num-servers', dest='init_num_servers', \
                        help="initial number of servers", \
                        action="store",
                        default = 10
                        )
    parser.add_argument('-f', '--logfile', dest='logfile', \
                        help="logfile for controller", \
                        action="store",
                        default = False
                        )
    parser.add_argument('-l', '--server-list-file', dest='server_list_file', \
                        help="servers list", \
                        action="store",
                        default = False
                        )
    parser.add_argument('-m', '--mem', dest='mem', \
                        help="memcached memory size", \
                        action="store",
                        default = 16
                        )

    args = parser.parse_args()

    if args.logfile:
        log_list += [open(args.logfile, 'w')]

    if not args.server_list_file:
        print("specify -l <server_list_file>")
        sys.exit(0)

    signal.signal(signal.SIGINT, ctrlC_handler)

    # moxi stop & execution
    init = []
    back = []
    lines = [line.strip() for line in open(args.server_list_file)]
    cnt = 0
    for h in lines:
        if cnt < int(args.init_num_servers):
            init.append(h)
        else:
            back.append(h)
        cnt += 1

    # loop to check the status to control the number of servers
    step = [100, 300, 500, 800, 1000, 1200, 1500, 1800, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000]
    stepmaxidx = 24
    WATCH_UP = 20
    WATCH_DOWN = 20
    curstep = 0
    curload = 0
    inccnt = 0
    deccnt = 0
    while True:
        if stopflag:
            break

        time.sleep(10)

        try:
            stat = get_sched_stat()
            statstr = stat.split(',')

            if len(statstr) < 3:
                continue

            curload = my_sum(statstr)
        except: 
            pass

        # only when ther are something..
        if curload == 0:
            continue

        if curload > step[curstep]:
            inccnt += 1
            deccnt = 0

        if curload < step[curstep]:
            deccnt += 1
            inccnt = 0

        # increasing
        if inccnt > WATCH_UP:
            if len(back) > 0:
                try:
                    node = back.pop()
                    add_node(node, args.mem)
                    init.append(node)
                except:
                    pass
                curstep += 1
                log("---a--- " + node + " added")
            inccnt = 0

        # increasing
        if deccnt > WATCH_DOWN:
            if len(init) > 0:
                # this needs to be more intelligent..
                try:
                    node = init.pop()
                    del_node(node)
                    back.append(node)
                except:
                    pass
                curstep -= 1
                log("---r--- " + node + " removed")
            deccnt = 0

        log("---c--- inc,dec,nservers,curload,curstep = " + str(inccnt) + "," + str(deccnt) + "," + str(len(init)) + "," + str(curload) + "," + str(step[curstep]))

        # safety...
        if curstep < 0:
            curstep = 0

        if curstep > stepmaxidx:
            curstep = stepmaxidx

    echo('Finished controller - exiting.')
    sys.exit(0)
