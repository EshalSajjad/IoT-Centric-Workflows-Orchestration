# deploy/oracle

Oracle VMs are set up with the generic VM tooling in [`../vm/`](../vm/README.md): fill in
[`../vm/cloud-init.example.yaml`](../vm/cloud-init.example.yaml) as the instance's cloud-init
script, then drive runs with `deploy/vm/orchflow_vm.py`.
