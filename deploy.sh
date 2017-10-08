#!/usr/bin/env bash
set -e
set -o pipefail

BIN=bin/monitor
NODES="ceph-mon ceph-osd0 ceph-osd1 ceph-osd2 ceph-osd3"
TARGET=/usr/bin/mira-ceph-monitor
LOG=/var/log/mira-ceph-monitor.log

SRV_FILE=mira-ceph-monitor.service
SERVICE=$SRV_FILE
SRV_FILE_DST_PATH=/lib/systemd/system/$SRV_FILE


function clean {
    set -x
    set +e
    for node in $NODES ; do
        CMD="systemctl stop $SERVICE ; systemctl disable $SERVICE"
        CMD="$CMD ; rm -f  $TARGET $LOG"
        echo "$CMD" | ssh $node sudo bash
    done
    set -e
}


function update_bin {
    set -x
    for node in $NODES ; do
        ssh $node sudo systemctl stop $SERVICE
        scp $BIN $node:/tmp/
        CMD="mv /tmp/monitor $TARGET && chown root.root $TARGET && chmod +x $TARGET && sudo systemctl start $SERVICE"
        echo "$CMD" | ssh $node sudo bash
    done
}


function deploy {
    set -x
    for node in $NODES ; do
        scp $BIN $node:/tmp/
        scp $SRV_FILE $node:/tmp

        CMD="sudo mv /tmp/monitor $TARGET && sudo chown root.root $TARGET && sudo chmod +x $TARGET"
        CMD="$CMD && mv /tmp/$SRV_FILE $SRV_FILE_DST_PATH"
        CMD="$CMD && chown root.root $SRV_FILE_DST_PATH"
        CMD="$CMD && systemctl daemon-reload && systemctl enable $SERVICE && systemctl start $SERVICE"
        echo "$CMD" | ssh $node sudo bash
    done
}

function show {
    set +x
    for node in $NODES ; do
        SRV_STAT=$(ssh $node sudo systemctl status $SERVICE | grep Active)
        echo $node " : " $SRV_STAT
    done
}

function tails {
    set +x
    for node in $NODES ; do
        echo $node
        ssh $node tail -n $1 $LOG
        echo
    done
}

set +x
while getopts "rcdsut:" opt; do
    case "$opt" in
    c)  clean
        ;;
    u)  update_bin
        ;;
    r)  clean
        deploy
        show
        ;;
    d)  deploy
        ;;
    s)  show
        ;;
    t)  tails $OPTARG
        ;;
    esac
done

