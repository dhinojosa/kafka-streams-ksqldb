# Cheat Sheet for Example

## Streams Example

```shell
./bin/kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic california_state_orders --from-beginning \
  --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --value-deserializer org.apache.kafka.common.serialization.IntegerDeserializer \
  --property print.key=true \
  --property key.separator=,
----
```
## KSQLDB Example

### Creating Stream

```ksql
CREATE STREAM my_avro_orders \
(total BIGINT, shipping VARCHAR, state VARCHAR, discount DOUBLE, \
gender VARCHAR) WITH (kafka_topic='my-avro-orders', value_format='AVRO');
```
### Viewing Contents

```shell
kafka-avro-console-consumer \
  --bootstrap-server broker:9092 \
  --topic next_day_shipping_by_state \
  --from-beginning \
  --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --property print.key=true \
  --property key.separator=,
```
