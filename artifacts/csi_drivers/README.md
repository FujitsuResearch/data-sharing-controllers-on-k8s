
## CSI driver supported by the volume controller

- [CephFS](https://github.com/ceph/ceph-csi)
- [Amazon Elastic File System](https://github.com/kubernetes-sigs/aws-efs-csi-driver)
- [Google Cloud Filestore](https://github.com/kubernetes-sigs/gcp-filestore-csi-driver)


## Create a CSI Driver docker image

Use the variable `${IMAGE_NAME}` if you would like to specify an image name.
```
$ IMAGE_NAME="[name]" ./csi_driver_image generate [cephfs|efs|filestore]
```

## Delete the CSI Driver docker image

Use the variable `${IMAGE_NAME}` if you would like to specify an image name.

```
$ IMAGE_NAME="[name]" ./csi_driver_image remove [cephfs|efs|filestore]
```
