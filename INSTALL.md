* git clone https://github.com:koder-ua/mira-ceph-mon.git
* cd mira-ceph-mon
* make unbz
* python3 -m venv mira-ceph-mon-env
* source mira-ceph-mon-env/bin/activate
* pip install -r requirements.txt
* Edit cfg.yaml - set you monitor nodes in 'mons' and monitoring
   storage path in 'storage' to non-volatile directory

* You can either use deploy.sh to manage services, or do this manually.
  For deploy.sh:
    - Edit deploy.sh file - set NODES variable to all mon/osd nodes
    - ssh to mon/osd nodes must be passwordless
    - sudo on mon/osd from ssh user must be passwordless
    - run './deploy.sh -r', it have to finish without errors and show that all services
      are running (in green)

  For manual installation:
    - copy 'bin/monitor' to '/usr/bin/mira-ceph-monitor' to all ceph nodes
    - copy 'mira-ceph-monitor.service' to /lib/systemd/system/ to all ceph nodes
    - register and start mira-ceph-monitor.service with
        * systemctl daemon-reload
        * systemctl enable mira-ceph-monitor.service
        * systemctl start mira-ceph-monitor.service
    - consult with steps from deploy.sh in case of issue

* run python client.py check cfg.yaml to check server's statuses
* run python client.py pool cfg.yaml
* navigate to http://MASTER_NODE_IP:8061 with browser

