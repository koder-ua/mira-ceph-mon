#!/usr/bin/env bash
set -xe
set -o pipefail

BIN=bin/monitor
NODES="ceph-mon ceph-osd0 ceph-osd1 ceph-osd2 ceph-osd3"
TARGET=/usr/bin/mira-ceph-monitor
LOG=/var/log/mira-ceph-monitor.log
SERVICE=mira-ceph-monitor.service


function clean {
    set +e
    for node in $NODES ; do
        ssh $node sudo systemctl stop $SERVICE
        ssh $node sudo systemctl disable $SERVICE
        ssh $node sudo rm -f /lib/systemd/system/mira-ceph-monitor.service $TARGET $LOG
    done
    set -e
}


function update_bin {
    for node in $NODES ; do
        ssh $node sudo systemctl stop $SERVICE
        scp $BIN $node:/tmp/
        ssh $node sudo bash -c "mv /tmp/monitor $TARGET ; chown root.root $TARGET ; chmod +x $TARGET ; sudo systemctl start $SERVICE"
    done

    set +x
    for node in $NODES ; do
        SRV_STAT=$(ssh $node sudo systemctl status $SERVICE | grep Active)
        echo $node " : " $SRV_STAT
    done
}


function deploy {
    for node in $NODES ; do
        scp $BIN $node:/tmp/

        ssh $node sudo mv /tmp/monitor $TARGET
        ssh $node sudo chown root.root $TARGET
        ssh $node sudo chmod +x $TARGET

        scp mira-ceph-monitor.service $node:/tmp

        ssh $node sudo mv /tmp/mira-ceph-monitor.service /lib/systemd/system
        ssh $node sudo chown root.root /lib/systemd/system/mira-ceph-monitor.service

        ssh $node sudo systemctl daemon-reload
        ssh $node sudo systemctl enable $SERVICE
        ssh $node sudo systemctl start $SERVICE
    done

    set +x
    for node in $NODES ; do
        SRV_STAT=$(ssh $node sudo systemctl status $SERVICE | grep Active)
        echo $node " : " $SRV_STAT
    done
}

function main {
    clean
    deploy
}

# update_bin
main
