# flink-git
Experiments with exactly-once streaming (using git semantics).

Flink's Kafka connector provides exactly-once guarantees when acting as a source (consumer) but not as a sink (producer) 
([reference](https://github.com/apache/flink/blob/c77e5ece581cbaaa423f38318d6735a222cf3cd5/flink-streaming-connectors/flink-connector-kafka-base/src/main/java/org/apache/flink/streaming/connectors/kafka/FlinkKafkaProducerBase.java#L53)).
While a Kafka source may rewind at ease to the offset tracked in the checkpoint state in the event of failure, Kafka provides no way to undo 
any records produced and thus rewind the sink.  This limitation invites the question of how to extend Kafka (or a similar system) to
provide exactly-once guarantees for a Kafka sink.  Since Kafka is envisioned as a commit log, may an answer be found in commit log concepts?
This repository explores that possibility.

Git provides a useful conceptual framework for the investigation, since its concepts are familiar and it is easily programmable 
with [jgit](https://eclipse.org/jgit/).   The flink-git repository is thus an experimental connector, based on jgit, that explores 
providing exactly-once guarantees as both a source and as a sink.

**Not intended for real applications.**

**Please use the [wiki](https://github.com/EronWright/flink-git/wiki) for discussion.**
