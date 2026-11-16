#!/usr/bin/env bash
# Prepare one Ubuntu VM (22.04 / 24.04, x86_64 or ARM64) to host Orchflow agents.
#
#   sudo bash bootstrap.sh                          # plain VM
#   sudo RUN_BROKER=1 bash bootstrap.sh             # the VM that also runs Mosquitto
#
# Safe to run again: every step checks before it changes anything.
# Settings come from the environment, or from /etc/orchflow/bootstrap.env if it exists
# (that is how cloud-init passes them, see cloud-init.example.yaml):
#
#   ORCHFLOW_REPO   git URL                    (default: the group repo)
#   ORCHFLOW_REF    branch, tag or commit      (default: main)
#   ORCHFLOW_DIR    where the repo is cloned   (default: /opt/orchflow)
#   ORCHFLOW_USER   SSH user that drives runs  (default: the sudo user, else ubuntu)
#   TS_AUTHKEY      Tailscale auth key; empty = log in by hand with `sudo tailscale up`
#   TS_HOSTNAME     name on the tailnet        (default: this machine's hostname)
#   RUN_BROKER      1 = run Mosquitto here     (default: 0)
#   BROKER_BIND     IP Mosquitto listens on    (default: this VM's Tailscale IP)
#   SWAP_GB         swapfile size, 0 = none    (default: 2)
set -euo pipefail

if [[ -f /etc/orchflow/bootstrap.env ]]; then
    # shellcheck disable=SC1091
    source /etc/orchflow/bootstrap.env
fi
ORCHFLOW_REPO=${ORCHFLOW_REPO:-https://github.com/EshalSajjad/IoT-Centric-Workflows-Orchestration.git}
ORCHFLOW_REF=${ORCHFLOW_REF:-main}
ORCHFLOW_DIR=${ORCHFLOW_DIR:-/opt/orchflow}
ORCHFLOW_USER=${ORCHFLOW_USER:-${SUDO_USER:-ubuntu}}
TS_AUTHKEY=${TS_AUTHKEY:-}
TS_HOSTNAME=${TS_HOSTNAME:-$(hostname)}
RUN_BROKER=${RUN_BROKER:-0}
BROKER_BIND=${BROKER_BIND:-}
SWAP_GB=${SWAP_GB:-2}
RESULTS_DIR=/var/lib/orchflow/results
IMAGE=orchflow:local

step() { echo; echo "==> $*"; }
die() { echo "bootstrap: $*" >&2; exit 1; }

[[ $EUID -eq 0 ]] || die "run as root: sudo bash bootstrap.sh"
command -v apt-get >/dev/null || die "only Ubuntu/Debian are supported (no apt-get)"
id "$ORCHFLOW_USER" >/dev/null 2>&1 || die "user '$ORCHFLOW_USER' does not exist; set ORCHFLOW_USER"

step "packages: docker, git, chrony"
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -y -q ca-certificates curl git docker.io chrony
systemctl enable --now docker chrony
# lets the driver run `docker ...` over SSH without sudo (applies to new SSH sessions)
usermod -aG docker "$ORCHFLOW_USER"

step "swap (${SWAP_GB} GB)"
if [[ "$SWAP_GB" -gt 0 ]] && ! swapon --show | grep -q .; then
    fallocate -l "${SWAP_GB}G" /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    grep -q '^/swapfile ' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
else
    echo "swap already on, or SWAP_GB=0"
fi

step "tailscale"
if ! command -v tailscale >/dev/null; then
    curl -fsSL https://tailscale.com/install.sh | sh
fi
if tailscale ip -4 >/dev/null 2>&1; then
    echo "already on the tailnet as $(tailscale ip -4)"
elif [[ -n "$TS_AUTHKEY" ]]; then
    tailscale up --authkey "$TS_AUTHKEY" --hostname "$TS_HOSTNAME"
else
    echo "NOT logged in. Run:  sudo tailscale up --hostname $TS_HOSTNAME"
fi
# Oracle's Ubuntu images end the INPUT chain with a REJECT rule. Let tailnet traffic in.
if iptables -S INPUT 2>/dev/null | grep -q -- '-j REJECT' \
        && ! iptables -C INPUT -i tailscale0 -j ACCEPT 2>/dev/null; then
    iptables -I INPUT 1 -i tailscale0 -j ACCEPT
    if command -v netfilter-persistent >/dev/null; then netfilter-persistent save; fi
    echo "allowed inbound traffic on tailscale0"
fi

step "repo ($ORCHFLOW_REF) in $ORCHFLOW_DIR"
if [[ ! -d "$ORCHFLOW_DIR/.git" ]]; then
    git clone -q "$ORCHFLOW_REPO" "$ORCHFLOW_DIR"
fi
chown -R "$ORCHFLOW_USER:$ORCHFLOW_USER" "$ORCHFLOW_DIR"
sudo -u "$ORCHFLOW_USER" git -C "$ORCHFLOW_DIR" fetch -q origin
if sudo -u "$ORCHFLOW_USER" git -C "$ORCHFLOW_DIR" rev-parse -q --verify "origin/$ORCHFLOW_REF" >/dev/null; then
    sudo -u "$ORCHFLOW_USER" git -C "$ORCHFLOW_DIR" checkout -q --detach "origin/$ORCHFLOW_REF"
else
    sudo -u "$ORCHFLOW_USER" git -C "$ORCHFLOW_DIR" checkout -q --detach "$ORCHFLOW_REF"
fi
echo "at $(git -C "$ORCHFLOW_DIR" rev-parse --short HEAD)"

step "docker image $IMAGE"
docker build -q -t "$IMAGE" -f "$ORCHFLOW_DIR/orchflow/deploy/local/Dockerfile" "$ORCHFLOW_DIR/orchflow"

mkdir -p "$RESULTS_DIR"
chown "$ORCHFLOW_USER:$ORCHFLOW_USER" "$RESULTS_DIR"

if [[ "$RUN_BROKER" == 1 ]]; then
    step "mosquitto"
    if [[ -z "$BROKER_BIND" ]]; then
        BROKER_BIND=$(tailscale ip -4 2>/dev/null | head -n1) \
            || die "no Tailscale IP yet: log in to Tailscale first, or set BROKER_BIND"
    fi
    # The broker has no password, so it must only listen on private addresses: the
    # Tailscale IP for the other VMs, loopback for agents on this VM (here our own
    # hostname resolves to 127.0.1.1, not the Tailscale IP).
    docker rm -f orchflow-broker >/dev/null 2>&1 || true
    docker run -d --restart unless-stopped --name orchflow-broker \
        -p "$BROKER_BIND:1883:1883" -p "127.0.0.1:1883:1883" eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf
    echo "mosquitto listening on $BROKER_BIND:1883 and 127.0.0.1:1883"
fi

step "done"
echo "host:    $TS_HOSTNAME ($(tailscale ip -4 2>/dev/null || echo 'not on tailnet'))"
echo "image:   $IMAGE   results: $RESULTS_DIR"
echo "Log out and back in (or reconnect SSH) so '$ORCHFLOW_USER' can use docker without sudo."
