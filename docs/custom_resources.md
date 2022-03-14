
## Custom resources for data life cycle

`xxx`s are user-specified values.

### `DataLifetime`

This custom resource is utilized to manage the time limit for provided data.  In addition, executables accessible to the provided data, *executables pass-listing*, are specified in this custom resource for file system data.

```
apiVersion: lifetimes.dsc/v1alpha1
kind: DataLifetime
metadata:
  name: xxx
spec:
  # trigger type is 'periodicSchedule' or 'cronSchedule'
  trigger:
    startTime: xxx    # optional, RFC 3339 format
    endTime: xxx      # RFC 3339 format
  DeletePolicy: xxx   # optional, 'chmod' or 'rm', default: chmod
  inputData:
  - messageQueueSubscrber:  # configurations for a message queue subscriber connected to the message queuing system running on the provider cluster
      messageQueueRef:  # reference to a 'MessageQueue' custom resource
        namespace: xxx
        name: xxx
      maxBatchBytes:    # optional, maximum size for one queue message, default: 1MB
    fileSystemSpec:
      fromPersistentVolumeClaimRef: xxx  # reference to a persistent volume claim for the volume including provided data in the 'data' container
      toPersistentVolumeClaimRef: xxx    # reference to a persistent volume claim for the local volume name in the 'data' container
      # optional
      allowedExecutables:
      - cmdAbsolutePath: xxx       # name of a command absolute path
        checksum: xxx              # optional, SHA-256 hash value
        writable: xxx              # optional, true if executables permit to write to data in the target volume
        # optional
        scripts:
        - absolutePath: xxx          # name of a script absolute path
          checksum: xxx              # optional, SHA-256 hash value
          writable: xxx              # optional, true if scripts permit to write to data in the target volume
          relativePathToInitWorkDir  # optional, relative path to an initial working directory if a script changes a working directory
    rdbSpec:   # [TODO]
      configurationRef:  # reference to a 'RdbData' custom resource
      ...
  outputData:
  - messageQueuePublisher:  # optional, configurations for a message queue publisher connected to the message queuing system running on the consumer cluster
      messageQueueRef:  # reference to a 'MessageQueue' custom resource
        namespace: xxx
        name: xxx
      maxBatchBytes:                    # optional, maximum size for one queue message, default: 1MB
      updatePublishChannelBufferSize:   # optional, channel buffer size for update publishing, default: 1,000
    fileSystemSpec:
      persistentVolumeClaimRef: xxx  # reference to a persistent volume claim for the output volume in the 'data' container
      # optional
      allowedExecutables:
      - cmdAbsolutePath: xxx       # name of a command absolute path
        checksum: xxx              # optional, SHA-256 hash value
        writable: xxx              # optional, true if executables permit to write to data in the target volume
    rdbSpec:   # [TODO]
      configurationRef:  # reference to a 'RdbData' custom resource
      ...
```

The `chmod` delete policy stands for removals of read, write, and execute permissions for files and directories.
The `rm` delete policy denotes deletions of files and directories.


### `MessageQueue`

This custom resource represents information on configurations for a publisher and a subscriber in a message queuing system.  This is referred from the `DataLifetime` instance.

```
apiVersion: core.dsc/v1alpha1
kind: MessageQueue
metadata:
  name: xxx
spec:
  brokers:
  - xxx     # broker addresses, e.g. 127.0.0.1:9092
  saslRef:  # reference to the 'Secret' resource for the SASL/SCRAM authentication configuration
    namespace: xxx
    name: xxx
```


### `RdbData`

This custom resource represents information on private configurations for input RDB data and output ones.  This is referred from the `DataLifetime` instance.

```
apiVersion: core.dsc/v1alpha1
kind: RdbData
metadata:
  name: xxx
spec:
  ...   # [TODO]
```
