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
    set +e

    RED='\033[0;31m'
    GREEN='\033[0;32m'
    NC='\033[0m' # No Color

    for node in $NODES ; do
        SRV_STAT=$(ssh $node sudo systemctl status $SERVICE | grep Active)
        if [[ $SRV_STAT == *" inactive "* ]]; then
            printf "%-20s : %b %s %b\n" "$node" "$RED" "$SRV_STAT" "$NC"
        else
            printf "%-20s : %b %s %b\n" "$node" "$GREEN" "$SRV_STAT" "$NC"
        fi
    done
}

function stop {
    set +x
    for node in $NODES ; do
        ssh $node sudo systemctl stop $SERVICE
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

function start {
    set -x
    for node in $NODES ; do
        ssh $node sudo systemctl start $SERVICE
    done
}

set +x
while getopts "rcdsut:ST" opt; do
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
    S)  start
        ;;
    T)  stop
        ;;
    esac
done

