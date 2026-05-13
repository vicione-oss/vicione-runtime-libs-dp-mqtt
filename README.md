# DataPort MQTT

## Node properties

| Name       | Incoming | Outgoing | Typ    | Values                                                |
|------------|:--------:|:--------:|--------|-------------------------------------------------------|
| Retain     |    ❌    |    ✔️    | `bool` | `true`/`false`                                        |
| Serializer |    ✔️    |    ✔️    | `byte` | `0`=Inherit from parent node, `1`=JSON, `2`=PlainText |
