import sys
import dbm
import time
import math
import logging
import argparse
import datetime
import threading
import functools
import socketserver
from http.server import SimpleHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor
from typing import List, Any, Tuple, Dict, Optional, Set
from numpy.polynomial.chebyshev import chebfit, chebval

import pygal
from pygal.style import DefaultStyle

import grpc
import yaml
import numpy

import stat_pb2
import stat_pb2_grpc


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def parse_args(args: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="YAML config file")
    return parser.parse_args(args)


def curr_hour() -> datetime.datetime:
    cd = datetime.datetime.now()
    return datetime.datetime(cd.year, cd.month, cd.day, cd.hour)


MaybeMonRpc = Optional[stat_pb2_grpc.SensorRPCStub]


def init_rpc_mon(addr: str, config: Dict[str, Any]) -> MaybeMonRpc:
    rpc = stat_pb2_grpc.SensorRPCStub(grpc.insecure_channel(addr))
    cluster = config['cluster']
    ids = config['osds'][addr]
    if ids:
        sett = stat_pb2.CephSettings(Cluster=cluster,
                                     OsdIDS=ids,
                                     HistorySize=config['history']['size'],
                                     HistoryTime=config['history']['time'],
                                     LatsCollectTimeout=max(config['collect_time'] // 2, 1),
                                     HistoMin=config['histogram']['min'],
                                     HistoMax=config['histogram']['max'],
                                     HistoBins=config['histogram']['bins'])
        rpc.SetupLatencyMonitoring(sett)
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


def coll_func(addr_rpc: Tuple[str, MaybeMonRpc], config: Dict[str, Any]) \
                        -> Tuple[MaybeMonRpc, str, Optional[stat_pb2.CephOpsLats]]:
    addr, rpc = addr_rpc
    try:
        if rpc is None:
            rpc = init_rpc_mon(addr, config)
    except Exception as exc:
        logger.error("During reconnect to %s: %s", addr, exc)
    else:
        try:
            return rpc, addr, rpc.GetCephOpsLats(stat_pb2.Empty())
        except Exception as exc:
            logger.error("During GetCephOpsLats from %s: %s", addr, exc)
            # close connection, will reconnect next time
            rpc = None

    return rpc, addr, None


def primary_info(prim_rpcs: Dict[str, MaybeMonRpc], config: Dict[str, Any]) -> Optional[stat_pb2.CephStatus]:
    for addr, prim_rpc in list(prim_rpcs.items()):
        if prim_rpc is None:
            prim_rpc = prim_rpcs[addr] = init_rpc_mon(addr, config)

        try:
            return prim_rpc.GetCephStatus(stat_pb2.Empty())
        except Exception as exc:
            logger.error("During GetCephStatus from %s, %s", addr, exc)
            prim_rpcs[addr] = None

    return None


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


def load_cycle(prim_rpcs: Dict[str, MaybeMonRpc], osd_rpc: Dict[str, MaybeMonRpc],
               config: Dict[str, Any], storage: Storage) -> None:
    empty_histo = lambda: numpy.zeros(config['histogram']['bins'], dtype=numpy.uint32)
    coll_func_cl = functools.partial(coll_func, config=config)
    plot_percentiles = numpy.array(config.get('plots', {}).get('percentiles', [50, 95, 99]), dtype=float) / 100
    # last_time_failed = set()  # type: Set[str]

    min_log = math.log10(config['histogram']['min'])
    max_log = math.log10(config['histogram']['max'])
    bin_vals = numpy.logspace(min_log, max_log, config['histogram']['bins'], base=10.0)

    # recalculate percentiles
    percentiles_dct = {}
    logger.info("Reloading percentiles")
    for name, vals_dct in storage.data.items():
        percentiles_dct[name] = {}
        # update percentiles
        for key, arrs in vals_dct.items():
            percentiles_dct[name][key] = tuple(get_perc_from_histo(arr, bin_vals, plot_percentiles)
                                               for arr in arrs)

    logger.info("Start monitoring")
    cthreads = min(config.get('collect_threads', 16), len(osd_rpc) + 1)
    logger.info("Spawn %d date collecting threads", cthreads)
    with ThreadPoolExecutor(cthreads) as pool:
        while True:
            collect_start_time = time.time()
            raggregated = empty_histo()
            waggregated = empty_histo()
            logger.debug("New scan cycle")
            prim_update_ft = pool.submit(primary_info, prim_rpcs, config)

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

            logger.debug("Get %d correct responces", ok_count)
            # last_time_failed = failed
            cluster_info = prim_update_ft.result()

            if not cluster_info:
                logger.warning("Fail to get cluster info")
            else:
                logger.debug("Get cluster info")

            ctime = int(time.time())
            for plt_info in config['plots']['plots']:
                curr_arr_key = ctime - (ctime % plt_info.step)
                dct = storage.data[plt_info.name]
                if curr_arr_key not in dct:
                    dct[curr_arr_key] = (raggregated, waggregated)

                    # remove last if needed
                    for rm_key in [evt_tm for evt_tm in dct if (evt_tm - ctime) >= plt_info.interval]:
                        storage.rm(plt_info.name, rm_key)
                        del percentiles_dct[plt_info.name][rm_key]
                else:
                    dct[curr_arr_key] = (dct[curr_arr_key][0] + raggregated, dct[curr_arr_key][1] + waggregated)
                    storage.sync_key(plt_info.name, curr_arr_key)

                # update percentiles
                percentiles_dct[plt_info.name][curr_arr_key] = \
                        tuple(get_perc_from_histo(arr, bin_vals, plot_percentiles) for arr in dct[curr_arr_key])

            storage.sync_db()
            t = time.time()
            ct = int(t)

            for plt_info in config['plots']['plots']:
                with graph_dict_lock:
                    prev_time, _ = graph_dict.get((plt_info.name, "read"), (0, None))

                if prev_time + plt_info.update_each < ct:
                    logger.info("Replotting %s", plt_info.name)
                    plot_graph(ct=ct,
                               data=percentiles_dct[plt_info.name],
                               plot_percentiles=plot_percentiles,
                               caption=plt_info.name,
                               ctime=ctime - (ctime % plt_info.step),
                               interval=plt_info.interval,
                               step=plt_info.step,
                               label_step=plt_info.label_step,
                               strftime_fmt=plt_info.strftime_fmt,
                               is_dots=config.get('plots', {}).get('dots', True),
                               curved_coef=config.get('plots', {}).get('curved_coef', 3))

            logger.debug("Rendering takes %.1f seconds", time.time() - t)
            sleep_time = config['collect_time'] - (time.time() - collect_start_time)
            if sleep_time > 0:
                time.sleep(sleep_time)


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
                    _, plot = graph_dict.get(key)

                if plot is not None:
                    self.send_response(200)
                    self.send_header('Content-type', 'image/svg+xml')
                    self.end_headers()
                    self.wfile.write(plot)
                    return

            self.send_response(404)
            self.end_headers()

    port = 8062
    httpd = socketserver.TCPServer(('localhost', port), MyRequestHandler)
    httpd.allow_reuse_address = True
    logger.info("Http server listened on http://%s:%s", 'localhost', port)
    httpd.serve_forever()


def main(argv: List[str]) -> int:
    args = parse_args(argv[1:])
    config = yaml.load(open(args.config))
    config['plots']['plots'] = [PlotParams(*dt) for dt in config['plots']['plots']]
    prim_rpcs = {}
    osd_rpc = {}

    for addr, osd_ids in config['osds'].items():
        if not osd_ids:
            prim_rpcs[addr] = None
        else:
            osd_rpc[addr] = None

    http_th = threading.Thread(target=http_thread, args=(config,))
    http_th.daemon = True
    http_th.start()

    storage = Storage(config)
    try:
        load_cycle(prim_rpcs, osd_rpc, config, storage)
    finally:
        for rpc in list(osd_rpc.values()) + list(prim_rpcs.values()):
            try:
                if rpc:
                    rpc.Stop(stat_pb2.Empty())
            except:
                pass
    return 0

if __name__ == "__main__":
    exit(main(sys.argv))
