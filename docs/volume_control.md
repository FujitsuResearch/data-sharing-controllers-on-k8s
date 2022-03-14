
## Volume Control

### Local Volumes

Directories for the volume control need to be prepared and cleaned up using the `shadowy-volumectl` command.

For example, the `shadowy-volumectl` command is issued in order to prepare the `/mnt/fuse/targets/vol1` directory.  `/mnt/fuse/sources/local/vol1` and `/mnt/fuse/targets` need to be prepared if these directories do not exist.
```
shadowy-volumectl init --volume-source-path /mnt/fuse/sources/local/vol1 --fuse-mounts-host-root-directory /mnt/fuse/targets --external-allowed-executables "[{\"command_absolute_path\": \"/usr/bin/kubelet\"}]"
```
The one of the `init` subcommand options is `--external-allowed-executables` in order to allow the external executables to directories for the volume control.  The external executables are ones belonging to different mount namespace from the *data* container and the *service* one.  Another command option is `--disable-usage-control`, which represents that usage control is disabled and data publishing is enabled.

The `final` subcommand is executed to clean up the directory for the volume control.
```
shadowy-volumectl final --fuse-mount-point-path /mnt/fuse/targets/vol1
```

### CSI Volumes

The volume controller decides whether to turn on usage control and data publishing or off them for CSI volumes according to their persistent volume claims.  To turn on them, you need to add their labels to `metadata` in a persistent volume claim instance as follows.

```
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ...
  labels:
    csi.volumes.dsc.io/usagecontrol: "true"  # to activate usage control
    csi.volumes.dsc.io/mqpublish: "true"     # to activate data publishing
spec:
  ...
```
