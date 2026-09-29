# Kafka share-group to Doris EOS proof

This opt-in test composes the experimental Kafka share acknowledgement sink with
the Doris Stream Load 2PC sink. It commits Doris first, acknowledges Kafka only
afterward, and audits a Duplicate Key table for the complete source-identity set
with multiplicity one.

It requires locally installed `flink-connector-kafka:5.1-SNAPSHOT` and
`kafka-clients:4.4.0-SNAPSHOT`, a KIP-1289 broker with `share.version=3` and
`transaction.version=2`, and reachable Doris FE, BE, and JDBC endpoints.

```shell
mvn -Pflink2,kafka-share-eos -pl flink-doris-connector-it -am \
  -Dkafka.share.doris.eos.it.enabled=true \
  -Dkafka.bootstrap.servers=127.0.0.1:19092 \
  -Ddoris.fenodes=127.0.0.1:18030 \
  -Ddoris.benodes=127.0.0.1:18040 \
  -Ddoris.jdbc.url=jdbc:mysql://127.0.0.1:19030 \
  -Dtest=KafkaShareToDorisEosITCase \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

A passing happy path is not EOS certification. Broker, TaskManager, JobManager,
Doris FE, Doris BE, retention-expiry, and autoscaling cases remain separate
required evidence.
