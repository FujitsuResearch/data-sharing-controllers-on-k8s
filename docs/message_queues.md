
## Message Queue Configurations

The [`MessageQueue`][lifecycle-cr] custom resource refers to the *Secret* resource, which contains a user name and a password in SASL/SCRAM authentication.

`xxx`s are user-specified values.


```
apiVersion: v1
kind: Secret
metadata:
  namespace: xxx
  name: xxx       # a user name
data:
  password: xxx   # encoded by base64
```


[lifecycle-cr]: ./custom_resources.md#messagequeue
