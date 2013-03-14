#!/usr/bin/env python
import commands, subprocess, os, sys, signal, time, re, itertools, pickle, telnetlib
#from vmstat import vmstat

# debug ------
verbose = True
stop = False

# help functions -----
echo_list = [sys.stdout]
def echo(s):
    for f in echo_list:
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
    stop = True
    print("here...");

def get_sched_stat():
    HOST = "localhost"
    PORT = "11211"
    tn = telnetlib.Telnet(HOST, PORT)
    tn.write("# stat\n")
    out = tn.read_some()
    tn.write("quit")

    # parsing here and return... put in the dic

    echo(out)
    return out

# command functions ------
def kill_moxi():
    (rc, out) = run_check('sudo pkill moxi')
    echo(out)

def kill_mcd(host):
    (rc, out) = run_check('ssh ubuntu@%s "sudo pkill memcached"' % host)
    echo(out)

def kill_controller():
    (rc, out) = run_check('sudo pkill moxi_controller.py' % host)
    echo(out)

def ex_moxi(opt, alpha, beta, mcdlist):
    t = '';
    if opt == 'dynamic':
        t = 'mcs_opts=distribution:dynamic'
    else:
        t = 'mcd_opts=distribution:ketama'

    kill_moxi()
    logfile = '/home/ubuntu/ec2.moxi.%s-%s-%s.log' % (opt, alpha, beta)
    out = run_background('/home/ubuntu/moxi/moxi/moxi -Z %s -q %s -w %s -z 11211=%s 2>%s ' % (t, alpha, beta, mcdlist, logfile))

def ex_mcd(host, mem):
    kill_mcd(host)
    (rc, out) = run_command('ssh ubuntu@%s "memcached -m %s < /dev/null >& /dev/null &"' % (host, mem))
    echo(out)

# XXX XXX XXX XXX XXX DEBUGGING
def ex_controller(initnum, listfile, mem, logfile):
    out = run_background('python moxi_controller.py -i %s -l %s -m %s -f %s ' % (initnum, listfile, mem, logfile))
    #print('python moxi_controller.py -i %s -l %s -m %s -f %s ' % (initnum, listfile, mem, logfile))

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
    parser.add_argument('-t', '--type', dest='type', \
                        help="dynamic | ketama", \
                        action="store",
                        default = 'dynamic'
                        )
    parser.add_argument('-a', '--alpha', dest='alpha', \
                        help="alpha value for scheduler", \
                        action="store",
                        default = 0.5
                        )
    parser.add_argument('-b', '--beta', dest='beta', \
                        help="beta value for scheduler", \
                        action="store",
                        default = 0.01
                        )
    parser.add_argument('-l', '--server-list-file', dest='server_list_file', \
                        help="servers list", \
                        action="store",
                        default = 'serverlist'
                        )
    parser.add_argument('-m', '--mem', dest='mem', \
                        help="memcached memory size", \
                        action="store",
                        default = 16
                        )

    args = parser.parse_args()

    if args.output_file:
        echo_list += [open(args.output_file, 'w')]

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

    # memcached stop & execution 
    for h in init:
        ex_mcd(h, args.mem)
    echo('All memcached are executed!')

    ex_moxi(args.type, args.alpha, args.beta, ",".join(init))
    echo('moxi is executed!')

    controllerlog = '/home/ubuntu/ec2.cont.%s-%s-%s.log' % (args.type, args.alpha, args.beta)

    if args.type == 'dynamic':
        ex_controller(args.init_num_servers, args.server_list_file, args.mem, controllerlog)

    # loop to check the status to control the number of servers
    #while True:
    #    if stop:
    #        break
    #    time.sleep(60)
    #    stat = get_sched_stat().split(',')

    #kill_moxi()

    echo('Finished - exiting.')
    sys.exit(0)
