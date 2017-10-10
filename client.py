import sys
import dbm
import json
import time
import math
import gzip
import logging
import argparse
import datetime
import threading
import functools
import socketserver
import urllib.request
from http.server import SimpleHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor
from typing import List, Any, Tuple, Dict, Optional, Set, Union

import pygal
from pygal.style import DefaultStyle
import grpc
import yaml
import numpy
from numpy.polynomial.chebyshev import chebfit, chebval

import stat_pb2
import stat_pb2_grpc


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def curr_hour() -> datetime.datetime:
    cd = datetime.datetime.now()
    return datetime.datetime(cd.year, cd.month, cd.day, cd.hour)


MaybeMonRpc = Optional[stat_pb2_grpc.SensorRPCStub]


DEFAULT_RPC_TIMEOUT = 5


def init_rpc_mon(addr: str, config: Dict[str, Any], is_osd: bool) -> MaybeMonRpc:
    rpc_timeout = config.get("rpc_timeout", DEFAULT_RPC_TIMEOUT)
    logger.debug("Connecting to %s", addr)
    rpc = stat_pb2_grpc.SensorRPCStub(grpc.insecure_channel(addr))
    if is_osd:
        cluster = config['cluster']
        ids = config['osds'][addr]
        sett = stat_pb2.CephSettings(Cluster=cluster,
                                     OsdIDS=ids,
                                     HistorySize=config['history']['size'],
                                     HistoryTime=config['history']['time'],
                                     LatsCollectTimeout=max(config['collect_time'] // 2, 1),
                                     HistoMin=config['histogram']['min'],
                                     HistoMax=config['histogram']['max'],
                                     HistoBins=config['histogram']['bins'])

        logger.debug("SetupLatencyMonitoring %s with osds=%r", addr, ids)
        rpc.SetupLatencyMonitoring(sett, timeout=rpc_timeout)
    return rpc


class Storage:
    max_del_count = 128

    def __init__(self, config: Dict[str, Any]) -> None:
        self.del_count = 0
        self.db = dbm.open(config['storage'], 'cf')

        # type: Dict[str, Dict[int, Tuple[numpy.ndarray, numpy.ndarray]]]
        self.data = {plt_cfg.name: {} for plt_cfg in config['plots']['plots']}

        logger.info("Scaning database")
        key = self.db.firstkey()

        while key:
            kname, utc = key.decode("utf8").rsplit(" ", 1)
            utc = int(utc)
            if kname in self.data:
                raw_arr = numpy.frombuffer(self.db[key], numpy.uint32)
                assert len(raw_arr) % 2 == 0
                self.data[kname][utc] = (raw_arr[:len(raw_arr) // 2], raw_arr[len(raw_arr) // 2:])
            else:
                logger.warning("Unexpected key %s in database", key)
            key = self.db.nextkey(key)

    def sync_key(self, name: str, key: int):
        arr1, arr2 = self.data[name][key]
        dbkey = "{} {}".format(name, key).encode("utf8")
        self.db[dbkey] = arr1.tobytes() + arr2.tobytes()

    def rm(self, name: str, key: int):
        dbkey = "{} {}".format(name, key).encode("utf8")
        del self.db[dbkey]
        del self.data[name][key]
        self.del_count += 1

        if self.del_count > self.max_del_count:
            self.db.reorganize()
            self.db.sync()
            self.del_count = 0

    def sync_db(self):
        self.db.sync()

    def __del__(self):
        self.db.close()


def get_perc_from_histo(bins: numpy.ndarray, bin_vals: numpy.ndarray,
                        percentiles: numpy.ndarray) -> Optional[numpy.ndarray]:

    assert sorted(percentiles) == list(percentiles)
    assert percentiles.max() <= 1.0 and percentiles.min() >= 0.0

    if bins.sum() < 1:
        return None

    cumbins = numpy.cumsum(bins) / bins.sum()
    pos = numpy.searchsorted(cumbins, percentiles)
    pos = pos.clip(0, len(bin_vals) - 1)

    return bin_vals[pos]


def pooling_stop_function(addr: str, config: Dict[str, Any]) -> None:
    rpc_timeout = config.get("rpc_timeout", DEFAULT_RPC_TIMEOUT)
    try:
        rpc = init_rpc_mon(addr, config, False)
        rpc.StopLatencyMonitoring(stat_pb2.Empty(), timeout=rpc_timeout)
    except Exception as exc:
        logger.error("During reconnect/stop %s: %s", addr, exc)


def latency_coll_func(addr_rpc: Tuple[str, MaybeMonRpc], config: Dict[str, Any]) \
                        -> Tuple[MaybeMonRpc, str, Optional[stat_pb2.CephOpsLats]]:
    rpc_timeout = config.get("rpc_timeout", DEFAULT_RPC_TIMEOUT)
    addr, rpc = addr_rpc
    try:
        if rpc is None:
            logger.error("Reiniting osd rpc to %s", addr)
            rpc = init_rpc_mon(addr, config, True)
    except Exception as exc:
        logger.error("During reconnect to %s: %s", addr, exc)
    else:
        try:
            return rpc, addr, rpc.GetCephOpsLats(stat_pb2.Empty(), timeout=rpc_timeout)
        except Exception as exc:
            logger.error("During GetCephOpsLats from %s: %s", addr, exc)
            # close connection, will reconnect next time
            rpc = None

    return rpc, addr, None


def cluster_info(prim_rpcs: Dict[str, MaybeMonRpc], config: Dict[str, Any]) -> Optional[stat_pb2.CephStatus]:
    rpc_timeout = config.get("rpc_timeout", DEFAULT_RPC_TIMEOUT)
    for addr, prim_rpc in list(prim_rpcs.items()):
        if prim_rpc is None:
            prim_rpc = prim_rpcs[addr] = init_rpc_mon(addr, config, False)

        try:
            return prim_rpc.GetCephStatus(stat_pb2.Empty(), timeout=rpc_timeout)
        except Exception as exc:
            logger.error("During GetCephStatus from %s, %s", addr, exc)
            prim_rpcs[addr] = None

    return None


def osdmap_info(prim_rpcs: Dict[str, MaybeMonRpc], config: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    rpc_timeout = config.get("rpc_timeout", DEFAULT_RPC_TIMEOUT)
    for addr, prim_rpc in list(prim_rpcs.items()):
        if prim_rpc is None:
            prim_rpc = prim_rpcs[addr] = init_rpc_mon(addr, config, False)

        try:
            res = prim_rpc.GetCephInfo(stat_pb2.Empty(), timeout=rpc_timeout)
        except Exception as exc:
            logger.error("During GetCephStatus from %s, %s", addr, exc)
            prim_rpcs[addr] = None
            continue

        if res.Compressed:
            return gzip.decompress(res.OsdDump).decode("utf8"), gzip.decompress(res.OsdTree).decode("utf8")
        else:
            return res.OsdDump, res.OsdTree

    return None, None


graph_dict_lock = threading.Lock()
graph_dict = {}  # type: Dict[Tuple[str, str], Tuple[int, bytes]]


class PlotParams:
    def __init__(self, name: str, interval: int, step: int,
                label_step: int, strftime_fmt: str, update_each: int) -> None:
        self.name = name
        self.interval = interval
        self.step = step
        self.label_step = label_step
        self.strftime_fmt = strftime_fmt
        self.update_each = update_each


def collect_lats_histos(config: Dict[str, Any],
                        pool: ThreadPoolExecutor,
                        osd_rpc: Dict[str, MaybeMonRpc]) -> Tuple[numpy.ndarray, numpy.ndarray]:

    empty_histo = lambda: numpy.zeros(config['histogram']['bins'], dtype=numpy.uint32)
    raggregated = empty_histo()
    waggregated = empty_histo()
    coll_func_cl = functools.partial(latency_coll_func, config=config)

    failed = set()  # type: Set[str]
    ok_count = 0
    for rpc, addr, olat in pool.map(coll_func_cl, list(osd_rpc.items())):
        osd_rpc[addr] = rpc
        if olat is None:
            failed.add(addr)
            continue

        ok_count += 1
        osd_data = list(olat.LatHisto)
        read_arr = empty_histo()
        read_arr[olat.RSkipped: olat.RSkipped + olat.RSize] = osd_data[:olat.RSize]
        write_arr = empty_histo()
        wdata = osd_data[olat.RSize:]
        write_arr[olat.WSkipped: olat.WSkipped + len(wdata)] = wdata

        raggregated += read_arr
        waggregated += write_arr

    logger.debug("Get %d correct latency responces from OSD's monitors", ok_count)
    return raggregated, waggregated


def update_database(storage: Storage,
                    raggregated:numpy.ndarray,
                    waggregated: numpy.ndarray,
                    plots: List[Any],
                    curr_time: int) -> Tuple[List[Tuple[str, int]], List[Tuple[str, int]]]:
    removed_keys = []  # type: List[Tuple[str, int]]
    updated_keys = []  # type: List[Tuple[str, int]]
    for plt_info in plots:
        curr_arr_key = curr_time - (curr_time % plt_info.step)
        dct = storage.data[plt_info.name]
        if curr_arr_key not in dct:
            dct[curr_arr_key] = (raggregated, waggregated)

            # remove last if needed
            for rm_key in [evt_tm for evt_tm in dct if (evt_tm - curr_time) >= plt_info.interval]:
                storage.rm(plt_info.name, rm_key)
                removed_keys.append((plt_info.name, rm_key))
        else:
            dct[curr_arr_key] = (dct[curr_arr_key][0] + raggregated, dct[curr_arr_key][1] + waggregated)
            storage.sync_key(plt_info.name, curr_arr_key)

        updated_keys.append((plt_info.name, curr_arr_key))

    storage.sync_db()
    return updated_keys, removed_keys


def get_osd_hosts(osd_map: str, use_public: bool = True) -> Dict[str, List[int]]:
    key = "public_addr" if use_public else "cluster_addr"
    res = {}  # type: Dict[str, List[int]]
    for osd_info in json.loads(osd_map)['osds']:
        addr = osd_info[key].split(":")[0]
        res.setdefault(addr, []).append(osd_info['osd'])
    return res


class OSDTreeItem:
    def __init__(self, tp: str, name: str, iid: int, children: List[int]) -> None:
        self.tp = tp
        self.name = name
        self.iid = iid
        self.children = children


def osds_for_roots(osd_tree: str) -> Dict[int, str]:
    nodes = {}  # type: Dict[int, OSDTreeItem]
    all_childs = set()  # type: Set[int]
    for node in json.loads(osd_tree)['nodes']:
        ti = OSDTreeItem(tp=node['type'], name=node['name'], iid=node['id'], children=node.get('children', []))
        nodes[ti.iid] = ti
        all_childs.update(node.get('children', []))

    roots = {}  # type: Dict[int, str]
    for root_node_iid in set(nodes) - all_childs:
        # find all osd's in this tree
        curr_nodes = set()
        new_curr = {root_node_iid}
        while curr_nodes != new_curr:
            curr_nodes = new_curr
            new_curr = set()
            for node_iid in curr_nodes:
                if not nodes[node_iid].children:
                    new_curr.add(node_iid)
                else:
                    new_curr.update(nodes[node_iid].children)

        assert all(nodes[iid].tp == 'osd' for iid in new_curr)
        root_name = nodes[root_node_iid].name
        for iid in new_curr:
            roots[iid] = root_name

    return roots


def update_cfg_with_osds_for_nodes(config: Dict[str, Any], mon_rpcs: Dict[str, MaybeMonRpc]):
    osds_config = config['osds']
    daemon_port = config.get("daemon_port", 5678)

    if 'osd_per_node' not in osds_config:
        return osds_config

    assert len(osds_config) == 1
    osd_per_node = osds_config['osd_per_node']
    assert isinstance(osd_per_node, int) or osd_per_node == 'all'

    osd_map, osd_tree = osdmap_info(mon_rpcs, config)
    if osd_map is None or osd_tree is None:
        logger.error("Failed to get osd map or tree")
        raise RuntimeError("Failed to get osd map or tree")

    roots4osd = osds_for_roots(osd_tree)
    roots = list(set(roots4osd.values()))
    if not roots:
        logger.error("Not found any crush root")
        raise RuntimeError("Not found any crush root")

    if len(roots) > 1:
        if "crush_root" not in config:
            logger.error("Need to set active crush root from %s in config via 'crush_root' option", ",".join(roots))
            raise RuntimeError("Need to set active crush root in config")
        active_root = config["crush_root"]
    else:
        active_root = roots[0]

    logger.debug("Using root %s", active_root)
    osds_for_nodes = {}  # type: Dict[str, Dict[str, List[int]]]
    for addr, osd_iids in get_osd_hosts(osd_map).items():
        osds_for_nodes[addr] = {}
        for osd_iid in osd_iids:
            osds_for_nodes[addr].setdefault(roots4osd[osd_iid], []).append(osd_iid)

    # select particular osd for monitoring
    osds_config = {}  # type: Dict[str, List[int]]
    for addr, osds_for_all_roots in osds_for_nodes.items():
        osds = osds_for_all_roots.get(active_root, [])
        rpc_addr = "{}:{}".format(addr, daemon_port)
        if osd_per_node == 'all':
            # need to select up to osd_per_node osd's from different roots
            osds_config[rpc_addr] = osds
        else:
            osds_config[rpc_addr] = osds[:osd_per_node]

    osd_configs_s = ['    "{}": [{}]'.format(addr, ",".join(map(str, osd_iids)))
                     for addr, osd_iids in sorted(osds_config.items())]
    logger.debug("Detected nodes configs:")
    for line in osd_configs_s:
        logger.debug(line)
    config['osds_orig'] = config['osds']
    config['osds'] = osds_config


def get_all_rpc(config: Dict[str, Any]) -> Tuple[Dict[str, MaybeMonRpc], Dict[str, MaybeMonRpc]]:
    monitors_rpc = {addr: None for addr in config['mons']}  # type: Dict[str, MaybeMonRpc]
    update_cfg_with_osds_for_nodes(config, monitors_rpc)
    osd_rpc = {addr: None for addr in config['osds']}
    return monitors_rpc, osd_rpc


def stop_rpcs(config: Dict[str, Any]):
    _, osd_rpc = get_all_rpc(config)
    with ThreadPoolExecutor(config.get('collect_threads', 16)) as pool:
        list(pool.map(functools.partial(pooling_stop_function, config=config), osd_rpc))


def check_rpc_servers(config: Dict[str, Any]):
    mons_rpc, osd_rpc = get_all_rpc(config)
    rpc_timeout = config.get("rpc_timeout", DEFAULT_RPC_TIMEOUT)

    def check_rpc(addr):
        try:
            rpc = init_rpc_mon(addr, config, False)
            rpc.GetCephStatus(stat_pb2.Empty(), timeout=rpc_timeout)
            return True, addr
        except:
            return False, addr

    def check_http(addr):
        try:
            with urllib.request.urlopen("http://{}:8062".format(addr.split(":")[0]), timeout=rpc_timeout) as fd:
                fd.read()
            return True, addr
        except:
            return False, addr


    with ThreadPoolExecutor(config.get('collect_threads', 16)) as pool:
        addrs = sorted(mons_rpc) + sorted(osd_rpc)

        print("RPC:")
        for is_ok, addr in pool.map(check_rpc, addrs):
            print("    {}: {}".format(addr, "OK" if is_ok else "FAIL"))

        print("HTTP:")
        for is_ok, addr in pool.map(check_http, addrs):
            print("    {}: {}".format(addr, "OK" if is_ok else "FAIL"))


def load_cycle(config: Dict[str, Any], storage: Storage) -> None:
    monitors_rpc, osd_rpc = get_all_rpc(config)
    try:
        cthreads = config.get('collect_threads', 16)
        with ThreadPoolExecutor(cthreads) as pool:
            logger.info("Spawning %d data collection threads", cthreads)

            plot_percentiles = numpy.array(config.get('plots', {}).get('percentiles', [50, 95, 99]), dtype=float) / 100

            min_log = math.log10(config['histogram']['min'])
            max_log = math.log10(config['histogram']['max'])
            bin_vals = numpy.logspace(min_log, max_log, config['histogram']['bins'], base=10.0)

            # calculate percentiles for historic data from database
            percentiles_dct = {}
            logger.info("Recalculating percentiles for historic data")
            for name, vals_dct in storage.data.items():
                percentiles_dct[name] = {}
                # update percentiles
                for key, arrs in vals_dct.items():
                    percentiles_dct[name][key] = tuple(get_perc_from_histo(arr, bin_vals, plot_percentiles)
                                                       for arr in arrs)

            logger.info("Start monitoring")

            while True:
                logger.debug("New scan cycle")
                collect_start_time = time.time()

                # get ceph health info
                info_ft = pool.submit(cluster_info, monitors_rpc, config)

                raggregated, waggregated = collect_lats_histos(config, pool, osd_rpc)

                info  = info_ft.result()
                if info is None:
                    logger.error("Failed to get cluster health status")
                    continue

                curr_time = int(time.time())

                # update database
                updated_keys, removed_keys = update_database(storage, raggregated, waggregated,
                                                             config['plots']['plots'], curr_time)

                # update percentiles
                for name, iid in removed_keys:
                    del percentiles_dct[name][iid]

                for name, iid in updated_keys:
                    percentiles_dct[name][iid] = tuple(get_perc_from_histo(arr, bin_vals, plot_percentiles)
                                                       for arr in storage.data[name][iid])

                # update plots
                plot_start_time = time.time()
                for plt_info in config['plots']['plots']:
                    with graph_dict_lock:
                        prev_time, _ = graph_dict.get((plt_info.name, "read"), (0, None))

                    if prev_time + plt_info.update_each < curr_time:
                        logger.debug("Replotting %s", plt_info.name)
                        plot_graph(ct=curr_time,
                                   data=percentiles_dct[plt_info.name],
                                   plot_percentiles=plot_percentiles,
                                   caption=plt_info.name,
                                   ctime=curr_time - (curr_time % plt_info.step),
                                   interval=plt_info.interval,
                                   step=plt_info.step,
                                   label_step=plt_info.label_step,
                                   strftime_fmt=plt_info.strftime_fmt,
                                   is_dots=config.get('plots', {}).get('dots', True),
                                   curved_coef=config.get('plots', {}).get('curved_coef', 3))

                logger.debug("Rendering takes %.1f seconds", time.time() - plot_start_time)

                sleep_time = config['collect_time'] - (time.time() - collect_start_time)
                if sleep_time > 0:
                    time.sleep(sleep_time)
    finally:
        logger.info("Closing rpc")
        for rpc in list(osd_rpc.values()) + list(monitors_rpc.values()):
            try:
                if rpc:
                    rpc.StopLatencyMonitoring(stat_pb2.Empty())
            except:
                pass


def approximate_curve(x: numpy.ndarray, y: numpy.ndarray, xnew: numpy.ndarray, curved_coef: int) -> numpy.ndarray:
    """returns ynew - y values of some curve approximation"""
    return chebval(xnew, chebfit(x, y, curved_coef))


def plot_graph(ct: int,
               data: Dict[int, Tuple[numpy.ndarray, numpy.ndarray]],
               plot_percentiles: numpy.ndarray, caption: str,
               ctime: int,
               interval: int,
               step: int,
               label_step: int,
               strftime_fmt: str,
               is_dots: bool = True,
               curved_coef: int = 3) -> None:

    vls_read = []
    vls_write = []
    labels = []
    maj_labels = []
    empty = [None] * len(plot_percentiles)
    stop_time = ctime - interval

    while ctime >= stop_time:
        read, write = data.get(ctime, (None, None))
        vls_read.append(empty if read is None else read)
        vls_write.append(empty if write is None else write)
        ctime -= step
        if ctime % label_step == 0:
            labl = datetime.datetime.fromtimestamp(ctime).strftime(strftime_fmt)
            labels.append(labl)
            maj_labels.append(labl)
        else:
            labels.append(str(ctime))

    for vls, tp in ((vls_read, "read"), (vls_write, "write")):
        perc_arrays = list(zip(*vls))
        settings = {
           "show_minor_x_labels": False,
           "x_label_rotation": 30,
           "legend_at_bottom": True,
           "legend_at_bottom_columns": 4,
           "style": DefaultStyle,
           "print_values": False,
           "margin_bottom": 50,
           "margin_right": 50
        }

        chart = pygal.Line(stroke=not is_dots, dots_size=1, **settings)

        if is_dots:
            for perc, arr in sorted(zip(plot_percentiles, perc_arrays)):
                chart.add('{}ppc'.format(int(perc * 100)), arr)
        else:
            for perc, arr in sorted(zip(plot_percentiles, perc_arrays)):
                new_x = numpy.arange(len(arr))
                filtered_x = numpy.array([i for i in range(len(arr)) if arr[i] is not None])
                filtered_y = numpy.array([i for i in arr if i is not None])
                if len(filtered_y) < curved_coef * 2:
                    continue
                new_y = approximate_curve(filtered_x, filtered_y, filtered_x, curved_coef=curved_coef)
                vmap = dict(zip(filtered_x, new_y))
                arr = [vmap.get(idx, None) for idx in new_x]
                chart.add('{}ppc'.format(int(perc * 100)), arr)

        chart.x_labels = labels
        chart.x_labels_major = maj_labels
        chart.title = 'OSD {} req latency: {}'.format(tp, caption)

        svg = chart.render()
        with graph_dict_lock:
            graph_dict[(caption, tp)] = (ct, svg)


def http_thread(config: Dict[str, Any]):

    url_map = {}
    urls = ""
    for plt_info in config['plots']['plots']:
        url_map["/" + plt_info.name.replace(" ", "_") + '_read'] = (plt_info.name, 'read')
        url_map["/" + plt_info.name.replace(" ", "_") + '_write'] = (plt_info.name, 'write')
        urls += '<a href="{}_read">{} read</a><br>'.format(plt_info.name.replace(" ", "_"), plt_info.name)
        urls += '<a href="{}_write">{} write</a><br>'.format(plt_info.name.replace(" ", "_"), plt_info.name)

    index_html = ("<html><body>" + urls + "</body></html>").encode("utf-8")

    class MyRequestHandler(SimpleHTTPRequestHandler):
        fname = '/tmp/data.svg'

        def do_GET(self):
            if self.path == '/' or self.path == '/index.html':
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(index_html)
                return
            else:
                key = url_map.get(self.path)

                with graph_dict_lock:
                    _, plot = graph_dict.get(key, (None, None))

                if plot is not None:
                    self.send_response(200)
                    self.send_header('Content-type', 'image/svg+xml')
                    self.end_headers()
                    self.wfile.write(plot)
                    return

            self.send_response(404)
            self.end_headers()

    ip, port = config.get('http', 'localhost:8061').split(":")
    port = int(port)
    socketserver.TCPServer.allow_reuse_address = True
    httpd = socketserver.TCPServer((ip, port), MyRequestHandler)
    logger.info("Http server listened on http://%s:%s", ip, port)
    httpd.serve_forever()


def parse_args(args: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=('pool', 'stop', 'check'), help="Action to run")
    parser.add_argument("config", help="YAML config file")
    return parser.parse_args(args)


def main(argv: List[str]) -> int:
    args = parse_args(argv[1:])
    config = yaml.load(open(args.config))

    if args.action == 'pool':
        config['plots']['plots'] = [PlotParams(*dt) for dt in config['plots']['plots']]
        http_th = threading.Thread(target=http_thread, args=(config,))
        http_th.daemon = True
        http_th.start()
        storage = Storage(config)
        load_cycle(config, storage)
    elif args.action == 'stop':
        stop_rpcs(config)
    elif args.action == 'check':
        check_rpc_servers(config)
    else:
        print("Unknown action", args.action)
        return 1
    return 0

if __name__ == "__main__":
    exit(main(sys.argv))
