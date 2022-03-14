
## External allowed executables

The JSON format of the `external-allowed-executables` argument for `shadowy-usagectl init` is as follows.

```
[
    {
        command_absolute_path: 'path',
        with_resident_command_absolute_path: 'path',
        writable: true,
        are_all_processes_allowed_in_namespace: true,
        is_host_process: true,
        mount_namespace_id: 'id',
        scripts: [
            {
                absolute_path: 'path',
                writable: true,
                relative_path_to_init_work_dir: 'path',
            },
            ...
        ]
    },
    ...
]
```

`command_absolute_path` is required while the others are optional.

The absolute path for an executable which is permitted to access provided data needs to be specified for `command_absolute_path`.

If command processes may not be currently running, you need to set `with_resident_command_absolute_path` to one of paths for command processes running in the same namespace, such as *daemon* processes.

If permissions for the write-related operations on provided data are granted to the executable, `writable: true` is required.

If you would like to give permissions on provided data to all processes running in the same mount namespace,  `are_all_processes_allowed_in_namespace` is chosen to be `true`.

If executable instances are host processes, `is_host_process` needs to be `true`.

If executable instances with the same path name exist in different mount namespaces on a server, a target executable for usage control is undecidable.  In this case, `mount_namespace_id` is required to clearly specify the target namespace.

`scripts` need to be specified if you would like to permit to execute scripts.  As with executables, `absolute_path` and `writable` are required for the absolute path for a script and whether write operations are passed, respectively.  If a script instance changes an initial working directory after start-up, you need to give `relative_path_to_init_work_dir` a relative path from the initial working directory.
