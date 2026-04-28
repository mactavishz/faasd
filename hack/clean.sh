#!/bin/bash

set -e

# Iterate over non-empty lines from stdin and run a callback command with each item.
for_each_line() {
    local callback=$1
    while IFS= read -r line; do
        [ -n "$line" ] || continue
        "$callback" "$line"
    done
}

# Run a ctr list command and extract the first column (skipping headers).
list_first_column() {
    local out
    out=$("$@" 2>/dev/null || true)
    [ -n "$out" ] || return 0
    printf '%s\n' "$out" | awk 'NR > 1 && NF > 0 { print $1 }'
}

# Wipe all resources in a containerd namespace
destroy_ctr_namespace() {
    local NS=$1

    remove_task() {
        local task_id=$1
        sudo ctr -n "$NS" task kill -s SIGKILL "$task_id" 2>/dev/null || true
        sudo ctr -n "$NS" task rm -f "$task_id" 2>/dev/null || true
    }

    remove_container() {
        local container_id=$1
        sudo ctr -n "$NS" c rm "$container_id" 2>/dev/null || true
    }

    remove_snapshot() {
        local snapshot_key=$1
        # Prefer "delete" for newer ctr versions, fallback to "rm" for older variants.
        sudo ctr -n "$NS" snapshots delete "$snapshot_key" 2>/dev/null || \
            sudo ctr -n "$NS" snapshots rm "$snapshot_key" 2>/dev/null || true
    }

    remove_image() {
        local image_ref=$1
        sudo ctr -n "$NS" i rm "$image_ref" 2>/dev/null || true
    }

    #  Check if namespace exists
    if ! list_first_column sudo ctr ns ls | grep -Fx "$NS" > /dev/null; then
        echo "Namespace '$NS' not found."
        return 0
    fi

    echo "--- Starting cleanup for namespace: $NS ---"

    # Kill and delete tasks (Running processes)
    # Tasks must be removed before containers
    local tasks
    tasks=$(list_first_column sudo ctr -n "$NS" task ls)
    if [ -n "$tasks" ]; then
        echo "Stopping active tasks..."
        printf '%s\n' "$tasks" | for_each_line remove_task
    fi

    # Delete containers
    local containers
    containers=$(list_first_column sudo ctr -n "$NS" c ls)
    if [ -n "$containers" ]; then
        echo "Deleting containers..."
        printf '%s\n' "$containers" | for_each_line remove_container
    fi

    if [ "$NS" == "openfaas" ]; then
        # Special handling for openfaas namespace
        # Tasks/containers are already removed above. For core faasd services, we keep
        # snapshots/images/namespace so rebuilds can reuse cached images and avoid
        # repeated pulls (and potential Docker Hub rate-limit failures).
        return 0
    fi

    # Delete snapshots (The writable layers)
    # Do not pass --snapshotter here for compatibility with older ctr builds.
    echo "Purging snapshots..."
    local snapshots
    snapshots=$(list_first_column sudo ctr -n "$NS" snapshots ls)
    if [ -n "$snapshots" ]; then
        printf '%s\n' "$snapshots" | for_each_line remove_snapshot
    fi

    # Delete images
    local images
    images=$(list_first_column sudo ctr -n "$NS" i ls)
    if [ -n "$images" ]; then
        echo "Deleting images..."
        printf '%s\n' "$images" | for_each_line remove_image
    fi

    # Delete the namespace
    echo "Finalizing: Removing namespace definition..."
    sudo ctr ns rm "$NS" 2>/dev/null || true

    echo "--- Purge Complete: '$NS' is gone. ---"
}

destroy_ctr_namespace "openfaas-fn"
destroy_ctr_namespace "openfaas"

# Clean up CNI network interfaces and configs
sudo ip link del openfaas0 2>/dev/null || true
sudo rm -rf /var/run/cni/openfaas-cni-bridge/*
sudo rm -f /etc/cni/net.d/10-openfaas.conflist
