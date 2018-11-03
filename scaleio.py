#!/usr/bin/python

__maintainer__ = "Abel Laura"
__email__ = "abel.laura@gmail.com"

import subprocess
import traceback
import types
import re
import json
import shlex
import getopt,sys

CONF = {
    'debug':          False,
    'verbose':        False,
    'scli_user':      'admin',
    'scli_password':  'admin',
    'mdm_ip':         '1.1.1.1',
    'pools':          [],
    'scli_cmd':      'scli',
    'ignoreselected': False,
}

POOLS_CAP = (
'NAME,MAX_CAPACITY_IN_KB,SPARE_CAPACITY_IN_KB,THIN_CAPACITY_ALLOCATED_IN_KB,'
'THICK_CAPACITY_IN_USE_IN_KB,UNUSED_CAPACITY_IN_KB,SNAP_CAPACITY_IN_USE_OCCUPIED_IN_KB,'
'CAPACITY_IN_USE_IN_KB,UNREACHABLE_UNUSED_CAPACITY_IN_KB,DEGRADED_HEALTHY_CAPACITY_IN_KB,'
'FAILED_CAPACITY_IN_KB,AVAILABLE_FOR_THICK_ALLOCATION_IN_KB,'
)

POOLS_PERF = (
'NAME,USER_DATA_READ_BWC,USER_DATA_WRITE_BWC,REBALANCE_READ_BWC,FWD_REBUILD_READ_BWC,BCK_REBUILD_READ_BWC,'
)

VOLUMES_CAP = (
'ID,NAME,SIZE,'
)

VOLUMES_PERF = (
'ID,NAME,USER_DATA_READ_BWC,USER_DATA_WRITE_BWC,'
)

SDSS_CAP = (
'ID,NAME,MAX_CAPACITY_IN_KB,MAX_CAPACITY_IN_KB,'
)

SDSS_PERF = (
'ID,NAME,TOTAL_READ_BWC,TOTAL_WRITE_BWC,'
)

SDCS_PERF = (
'ID,NAME,USER_DATA_READ_BWC,USER_DATA_WRITE_BWC,'
)

SDCS_CAP = (
'ID,NAME,IP,NUM_OF_MAPPED_VOLUMES,OS_TYPE,'
)

DEVICE_HEALTH = (
'SDS_ID,NAME,STATE,ERR_STATE,'
)

class AutoVivification(dict):
    """Implementation of perl's autovivification feature."""
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value

def check_output(cmd):
    try:
        out=subprocess.Popen(cmd, stdout=subprocess.PIPE)
        output = out.communicate()[0]
    except Exception as e:
        print('Error on executing command %s --- %s' %(e, traceback.format_exc()))
        exit(1)
    return output

def sclio_login():
    login_cmd = (CONF['scli_cmd'] + " --login --username=" + CONF['scli_user'] + " --password='" + CONF['scli_password'] + "' --mdm_ip=" + CONF['mdm_ip'])
    my_debug(login_cmd)
    out = check_output(shlex.split(login_cmd))
    my_debug(out)

    if 'Logged in' in out:
        return 1
    else:
        return 0

def sclio_logout():
    logout_cmd = ("scli --logout --mdm_ip=" + CONF['mdm_ip'])
    out = check_output(shlex.split(logout_cmd))
    my_debug(out)

def dispatch_value(plugin, value, plugin_instance=None, type_instance=None):
    print('%s,%s=%s %s=%s' % ('scaleio_' + plugin, plugin, plugin_instance, type_instance, value))

#Example output example,tag1=a,tag2=b i=42i,j=43i,k=44i
def dispatch_value_ex(label, label_val, tag1, tag1_val, i=None, i_val=None):
    print('%s,%s=%s,%s=%s %s=%s' % ('scaleio_' + label, label, label_val,tag1 ,tag1_val, i, i_val ))

def get_sdc(opt_params):
    if opt_params == 1 :
        sdcs = read_properties('--query_properties', '--object_type', 'SDC', '--all_objects',
        '--properties', SDCS_PERF)
    elif opt_params == 2 :
        sdcs = read_properties('--query_properties', '--object_type', 'SDC', '--all_objects',
        '--properties', SDCS_CAP)
    else :
        sdcs = read_properties('--query_properties', '--object_type', 'SDC', '--all_objects',
        '--properties', SDCS_PERF+SDCS_CAP)
    if sdcs == None:
        return
    for sdc_id, sdc in sdcs.iteritems():
        if opt_params!=1 : dispatch_value('sdc', long(sdc['NUM_OF_MAPPED_VOLUMES']), sdc['NAME'], 'NUM_OF_MAPPED_VOLUMES')
        if opt_params!=2 : dispatch_value('sdc', long(sdc['USER_DATA_READ_BWC']['IOPS']), sdc['NAME'], 'USER_DATA_READ_BWC_IOPS')
        if opt_params!=2 : dispatch_value('sdc', long(sdc['USER_DATA_WRITE_BWC']['IOPS']), sdc['NAME'], 'USER_DATA_WRITE_BWC_IOPS')
        if opt_params!=2 : dispatch_value('sdc', long(sdc['USER_DATA_READ_BWC']['BPS']), sdc['NAME'], 'USER_DATA_READ_BWC_BPS')
        if opt_params!=2 : dispatch_value('sdc', long(sdc['USER_DATA_WRITE_BWC']['BPS']), sdc['NAME'], 'USER_DATA_WRITE_BWC_BPS')

def get_sds(opt_params):
    if opt_params == 1 :
        sdss = read_properties('--query_properties', '--object_type', 'SDS', '--all_objects',
        '--properties', SDSS_PERF)
    elif opt_params == 2 :
        sdss = read_properties('--query_properties', '--object_type', 'SDS', '--all_objects',
        '--properties', SDSS_CAP)
    else :
        sdss = read_properties('--query_properties', '--object_type', 'SDS', '--all_objects',
        '--properties', SDSS_PERF+SDSS_CAP)
    if sdss == None:
        return
    for sds_id, sds in sdss.iteritems():
        if opt_params!=1 : dispatch_value('sds', long(sds['MAX_CAPACITY_IN_KB']) / 2, sds['NAME'], 'MAX_CAPACITY_IN_KB_RAW')
        if opt_params!=2 : dispatch_value('sds', long(sds['TOTAL_READ_BWC']['IOPS']), sds['NAME'], 'TOTAL_READ_BWC_IOPS')
        if opt_params!=2 : dispatch_value('sds', long(sds['TOTAL_WRITE_BWC']['IOPS']), sds['NAME'], 'TOTAL_WRITE_BWC_IOPS')
        if opt_params!=2 : dispatch_value('sds', long(sds['TOTAL_READ_BWC']['BPS']), sds['NAME'], 'TOTAL_READ_BWC_BPS')
        if opt_params!=2 : dispatch_value('sds', long(sds['TOTAL_WRITE_BWC']['BPS']), sds['NAME'], 'TOTAL_WRITE_BWC_BPS')

def get_volumes(opt_params):
    if opt_params == 1 :
        volumes = read_properties('--query_properties', '--object_type', 'VOLUME', '--all_objects',
        '--properties', VOLUMES_PERF)
    elif opt_params == 2 :
        volumes = read_properties('--query_properties', '--object_type', 'VOLUME', '--all_objects',
        '--properties', VOLUMES_CAP)
    else :
        volumes = read_properties('--query_properties', '--object_type', 'VOLUME', '--all_objects',
        '--properties', VOLUMES_CAP + VOLUMES_PERF)

    if volumes == None:
        return
    for volume_id, volume in volumes.iteritems():
        if opt_params!=1 : dispatch_value('volume', long(volume['SIZE']), volume['NAME'], 'SIZE')
        if opt_params!=2 : dispatch_value('volume', long(volume['USER_DATA_READ_BWC']['IOPS']), volume['NAME'], 'USER_DATA_READ_BWC_IOPS')
        if opt_params!=2 : dispatch_value('volume', long(volume['USER_DATA_WRITE_BWC']['IOPS']), volume['NAME'], 'USER_DATA_WRITE_BWC_IOPS')
        if opt_params!=2 : dispatch_value('volume', long(volume['USER_DATA_READ_BWC']['BPS']), volume['NAME'], 'USER_DATA_READ_BWC_BPS')
        if opt_params!=2 : dispatch_value('volume', long(volume['USER_DATA_WRITE_BWC']['BPS']), volume['NAME'], 'USER_DATA_WRITE_BWC_BPS')

def get_disks(opt_params):
    if opt_params == 3 :
        disks = read_properties('--query_properties','--object_type','DEVICE', '--all_objects',
        '--properties', DEVICE_HEALTH)
    if disks == None:
        return

    sdss = read_properties('--query_properties', '--object_type', 'SDS', '--all_objects',
        '--properties', 'NAME')
    if sdss == None:
        return    

    for disk_id, disk in disks.iteritems():
        dispatch_value_ex('disk',disk['NAME'],'sds',sdss[disk['SDS_ID']]['NAME'], 'failed',err2bool(disk['ERR_STATE']))

# Query ScaleIO for pool metrics.
def get_pools(opt_params):
    if opt_params == 1 :
        pools = read_properties('--query_properties', '--object_type', 'STORAGE_POOL', '--all_objects',
        '--properties', POOLS_PERF)
    elif opt_params == 2 :
        pools = read_properties('--query_properties', '--object_type', 'STORAGE_POOL', '--all_objects',
                '--properties', POOLS_CAP)
    else :
        pools = read_properties('--query_properties', '--object_type', 'STORAGE_POOL', '--all_objects',
        '--properties', POOLS_CAP + POOLS_PERF)

    # We have nothing to report
    if pools == None:
        return

    for pool_id, pool in pools.iteritems():
        # skip pools based on configuration
        if len(CONF['pools']) > 0 and not CONF['ignoreselected'] and pool['NAME'] not in CONF['pools']:
            my_verbose('Pool %s is not in pools configuration and ignoreselected is false -> skipping' % (pool['NAME']))
            continue
        if len(CONF['pools']) > 0 and CONF['ignoreselected'] and pool['NAME'] in CONF['pools']:
            my_verbose('Pool %s is in pools configuration and ignoreselected is true -> skipping' % (pool['NAME']))
            continue

        # raw capacity
        if opt_params!=1 : dispatch_value('pool', long(pool['MAX_CAPACITY_IN_KB']) / 2, pool['NAME'], 'MAX_CAPACITY_IN_KB_RAW')

        # useable capacity
        if opt_params!=1 : dispatch_value('pool',
            long(pool['AVAILABLE_FOR_THICK_ALLOCATION_IN_KB']) + long(pool['CAPACITY_IN_USE_IN_KB']) / 2,
            pool['NAME'], 'USABLE_CAPACITY_IN_KB')

        # available capacity
        if opt_params!=1 : dispatch_value('pool',
            long(pool['AVAILABLE_FOR_THICK_ALLOCATION_IN_KB']),
            pool['NAME'], 'AVAILABLE_FOR_ALLOCATION_IN_KB')

        # used capacity
        if opt_params!=1 : dispatch_value('pool', (long(pool['CAPACITY_IN_USE_IN_KB'])) / 2, pool['NAME'], 'CAPACITY_IN_USE_IN_KB')

        # allocated capacity
        if opt_params!=1 : dispatch_value('pool',
            (long(pool['THIN_CAPACITY_ALLOCATED_IN_KB']) +
                long(pool['THICK_CAPACITY_IN_USE_IN_KB']) + long(pool['SNAP_CAPACITY_IN_USE_OCCUPIED_IN_KB'])) / 2,
            pool['NAME'], 'ALLOCATED_CAPACITY_IN_KB')

        # unreachable unused capacity
        if opt_params!=1 : dispatch_value('pool', long(pool['UNREACHABLE_UNUSED_CAPACITY_IN_KB']) / 2, pool['NAME'], 'UNREACHABLE_UNUSED_CAPACITY_IN_KB')

        # degraded capacity
        if opt_params!=1 : dispatch_value('pool', long(pool['DEGRADED_HEALTHY_CAPACITY_IN_KB']), pool['NAME'], 'DEGRADED_HEALTHY_CAPACITY_IN_KB')

        # failed capacity
        if opt_params!=1 : dispatch_value('pool', long(pool['FAILED_CAPACITY_IN_KB']) / 2, pool['NAME'], 'FAILED_CAPACITY_IN_KB')

        # spare capacity
        if opt_params!=1 : dispatch_value('pool', long(pool['SPARE_CAPACITY_IN_KB']), pool['NAME'], 'SPARE_CAPACITY_IN_KB')

        # user data read IOPS
        if opt_params!=2 : dispatch_value('pool', long(pool['USER_DATA_READ_BWC']['IOPS']), pool['NAME'], 'USER_DATA_READ_BWC_IOPS')

        # user data read throughput
        if opt_params!=2 : dispatch_value('pool', long(pool['USER_DATA_READ_BWC']['BPS']), pool['NAME'], 'USER_DATA_READ_BWC_BPS')

        # user data write IOPS
        if opt_params!=2 : dispatch_value('pool', long(pool['USER_DATA_WRITE_BWC']['IOPS']), pool['NAME'], 'USER_DATA_WRITE_BWC_IOPS')

        # user data write throughput
        if opt_params!=2 : dispatch_value('pool', long(pool['USER_DATA_WRITE_BWC']['BPS']), pool['NAME'], 'USER_DATA_WRITE_BWC_BPS')

        # rebalance IOPS
        if opt_params!=2 : dispatch_value('pool', long(pool['REBALANCE_READ_BWC']['IOPS']), pool['NAME'], 'REBALANCE_READ_BWC_IOPS')

        # rebalance throughput
        if opt_params!=2 : dispatch_value('pool', long(pool['REBALANCE_READ_BWC']['BPS']), pool['NAME'], 'REBALANCE_READ_BWC_BPS')

        # rebuild IOPS
        if opt_params!=2 : dispatch_value('pool',
            long(pool['FWD_REBUILD_READ_BWC']['IOPS'])  +
                long(pool['BCK_REBUILD_READ_BWC']['IOPS']),
            pool['NAME'], 'REBUILD_IOPS')

        # rebuild throughput
        if opt_params!=2 : dispatch_value('pool',
            long(pool['FWD_REBUILD_READ_BWC']['BPS']) +
                long(pool['BCK_REBUILD_READ_BWC']['BPS']),
            pool['NAME'], 'REBUILD_BPS')

# Execute a scli --query_properties command and convert the CLI output to a dict/JSON
def read_properties(*cmd):
    properties = AutoVivification()
    out = None
    real_cmd = (CONF['scli_cmd'],'--mdm_ip='+CONF['mdm_ip']) + cmd
    my_debug('Executing command: %s %s%s %s' % (CONF['scli_cmd'], '--mdm_ip=', CONF['mdm_ip'], " ".join(str(v) for v in cmd)))
#        out = subprocess.check_output(real_cmd, stderr=subprocess.STDOUT)
    out = check_output(real_cmd)

    if 'Failed to connect to MDM' in out:
        my_verbose('Failed to connect to MDM, skipping data collection')

    group_name = None
    group_regex = re.compile("^([^\s]+)\s([^:]+)")
    kv_regex = re.compile("^\s+([^\s]+)\s+(.*)$")
    for line in out.split('\n'):
        new_group_match = group_regex.match(line)
        if new_group_match:
            group_name = new_group_match.group(2)
        else:
            kv_match = kv_regex.match(line)
            if kv_match:
                properties[group_name][kv_match.group(1)] = kv_match.group(2)

    my_verbose('Read properties: %s' % (json.dumps(properties)))
    rectify_dict(properties)
    my_debug('Properties after rectify: %s' % (json.dumps(properties)))
    return properties

# Recitify the properties read from the command line:
#  - convert units such as KB,MB,GB to bytes
#  - interpret the BWC values and extract IOPS, Throughput
def rectify_dict(var):
    for key, val in var.iteritems():
        if type(val) is dict or type(val) is AutoVivification:
            rectify_dict(val)
        elif type(val) is str:
            if key.endswith('BWC'):
                var[key] = convert_bwc_to_dict(val)
            else:
                var[key] = convert_units_to_bytes(val)

def convert_bwc_to_dict(val):
    m = re.search('([0-9]+) IOPS (.*) per-second', val, re.I)
    return {'IOPS': m.group(1), 'BPS': convert_units_to_bytes(m.group(2))}

def convert_units_to_bytes(val):
    val = convert_unit_to_bytes(val, "Bytes", 0)
    val = convert_unit_to_bytes(val, "KB", 1)
    val = convert_unit_to_bytes(val, "MB", 2)
    val = convert_unit_to_bytes(val, "GB", 3)
    val = convert_unit_to_bytes(val, "TB", 4)
    val = convert_unit_to_bytes(val, "PB", 5)
    return val

def convert_unit_to_bytes(val, unit, power):
    m = re.search('([0-9\.]+) ' + unit, val, re.I)
    if m:
        return str(long(m.group(1)) * (1024 ** power))
    return val

def str2bool(v):
    if type(v) == types.BooleanType:
        return v
    return v.lower() in ("yes", "true", "t", "1")

def err2bool(v):
    if type(v) == types.BooleanType:
        return v
    return int(v.lower() in ("error"))
def my_debug(msg):
    if CONF['debug']:
        print('ScaleIO: %s' % (msg))

def my_verbose(msg):
    if CONF['verbose']:
        print('ScaleIO: %s' % (msg))

def main(argv):

    if len(sys.argv) == 1:
        exit()
    try:
        opts, args = getopt.getopt(sys.argv[1:],"pcha")
    except getopt.GetoptError as err:
        print err
        sys.exit(2)

    capacity=False
    performance=False
    health=False
    params=0
    for o, a in opts:
        if o == "-p":
            performance = True
            params=1
        elif o in ("-c"):
            capacity = True
            params=2
        elif o in ("-h"):
            health = True
            params=3
        elif o in ("-a"):
            capacity = True
            performance = True
            health = True
            params=4
        else:
            assert False, "Nothing to do"
            exit()
    if (sclio_login()==1):
	    if params == 3:
            get_disks(params)
        else:
            get_pools(params)
            get_volumes(params)
            get_sds(params)
            get_sdc(params)
	
        sclio_logout()
    else:
        print('Error: Login failed')

if __name__ == "__main__":
    main(sys.argv[1:])

