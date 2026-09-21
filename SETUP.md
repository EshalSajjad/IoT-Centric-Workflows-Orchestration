# Distributed Testbed Setup (beyond the original README)
---
## 1. Java version

**JDK 8 specifically.** Newer Java breaks the bundled Paho MQTT / Moquette jars. Not mentioned in the original README — install a JDK 8 distribution (e.g. Temurin/Adoptium) and use it explicitly for this project if your machine's default `java` is newer.

## 2. Install Tailscale (laptop + both VMs)

This project's distributed setup relies on a Tailscale mesh network rather than public IPs/port-forwarding, since a home laptop isn't reachable from the public internet by default.

1. Install on your laptop: [tailscale.com/download](https://tailscale.com/download) (or `winget install Tailscale.Tailscale` on Windows), then `tailscale up` and log in.
2. On each VM: `curl -fsSL https://tailscale.com/install.sh | sh` then `sudo tailscale up --hostname=<name>`, and log in with the **same Tailscale account** so all machines land on one tailnet.
3. Run `tailscale status` on any machine to see all three devices and their `100.x.y.z` addresses — you'll need these in step 5.

## 3. Fix the jar/lib folder-name mismatch (one-time, per VM)

The prebuilt `peer-node-gom.jar` / `end-device.jar` expect lib folder names that don't match what's shipped in `execuateable-jars/`. After copying the relevant folder to each VM, create a symlink so the jar can find its dependencies:

On the peer-node VM, inside the copied `peer-node-gom/` folder:
```
ln -sf peer-node-gom_lib peer-node_lib
```

On the end-device VM, inside the copied `end-device/` folder:
```
ln -sf end-device_lib Main_Rasp_IoT_lib
```
Without this you'll get `NoClassDefFoundError` on startup.

## 4. Additional database setup (beyond the base `gom.sql` import)

1. Widen the SFTP password column (it now holds a private key file path, not a short password):
   ```sql
   ALTER TABLE devices MODIFY sftp_password VARCHAR(255);
   ```
2. Point the seed `devices` and `peer_nodes` rows at **your own** VMs' Tailscale IPs (not anyone else's — use `tailscale status` from step 3 to get yours):
   ```sql
   UPDATE devices SET IP_Address = '<your-end-device-tailscale-ip>',
     sftp_username = 'ubuntu',
     sftp_password = '<absolute path to your own SSH private key file>',
     mqtt_port = '1883';
   UPDATE peer_nodes SET IP_Address = '<your-peer-node-tailscale-ip>', mqtt_port = '1883';
   ```
   The key path is a real file on *your* machine — do not reuse anyone else's key or path.
3. Fix the deployment source/destination paths (see step 5 — the original values point at files that don't exist on anyone's machine):
   ```sql
   UPDATE deployment SET
     Source_File_Address_1 = '<repo-path>/deployment-source-files/data_integration_service.jar',
     Source_File_Address_2 = '<repo-path>/deployment-source-files/Aloha_0.jpg',
     Source_File_Address_3 = '<repo-path>/deployment-source-files/config.txt',
     Source_File_Address_4 = '<repo-path>/deployment-source-files/output_0.kml',
     Destination_File_Address_1 = '/home/ubuntu/<your-end-device-folder-name>/data_integration_service.jar',
     Destination_File_Address_2 = '/home/ubuntu/<your-end-device-folder-name>/Aloha_0.jpg',
     Destination_File_Address_3 = '/home/ubuntu/<your-end-device-folder-name>/config.txt',
     Destination_File_Address_4 = '/home/ubuntu/<your-end-device-folder-name>/output_0.kml'
   WHERE Deployment_Service_Id IN (128,135,142);
   ```

## 5. The stub deployment files

The real `data_integration_service.jar` (and its companion files) weren't included in the shared GitHub repo and were only referenced by absolute paths on the original researchers' own machine. A set of small, harmless placeholder files (no real plume-modeling logic, just stand-ins so the deployment step has something real to transfer) ships with this repo at `deployment-source-files/` — no need to ask anyone for anything, it comes with your `git clone`/`git pull`. Just point `Source_File_Address_1-4` (step 3 above) at wherever you cloned the repo, e.g. `<repo-path>/deployment-source-files/data_integration_service.jar`.

## 6. Set `deployment_path` in `config.properties` to match where YOUR end-device process runs from

```
deployment_path = /home/ubuntu/<your-end-device-folder-name>/
```
This must be a real absolute Linux path (not Windows-style), and must match the exact folder you launch `end-device.jar` from on your VM — the coordinator's file-deployment code uses this single value both to create the remote folder and to decide where to put files, so if it's wrong, files get created in one place and searched for in another.

## 7. Extra notes

- start MySQL, then start peer-node-gom on its VM, then start end-device on its VM, and then start the coordinator last. Starting the coordinator before the peer-node is listening causes a one-time "Client is not connected" error on its first publish (it recovers later, but starting correctly is better).
- Kill stale processes before every relaunch. `taskkill /F /IM java.exe` (Windows) or `pgrep -fl 'java -jar'` then `kill <pid>` (Linux VM) before trying again.