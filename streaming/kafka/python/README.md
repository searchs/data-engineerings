# Kafka with Python

Curated from the legacy `streaming-with-kafka` repository.

## What the original lab covered

- basic producer and consumer flows;
- topic creation through the admin client;
- JSON event serialisation;
- Avro schemas;
- Schema Registry integration;
- generated purchase/click events.

## Producer pattern

The original producer used `confluent-kafka-python`, but it also mixed topic creation, event generation and an infinite producer loop in one file. A cleaner structure is:

```python
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from confluent_kafka import Producer


@dataclass(frozen=True)
class Purchase:
    username: str
    currency: str
    amount: int

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "purchase-producer",
            "compression.type": "lz4",
            "linger.ms": 100,
        }
    )


def publish(producer: Producer, topic: str, purchase: Purchase) -> None:
    producer.produce(topic, value=purchase.to_bytes())
    producer.poll(0)
```

Keep topic-administration responsibilities separate from the producer itself, and flush on controlled shutdown rather than hiding it inside an unbounded loop.

## Schema Registry / Avro lesson

The historical example used the older `confluent_kafka.avro` API. The durable design lesson is still useful:

1. define the event schema independently of transport code;
2. register/resolve schemas through Schema Registry;
3. serialise producer values against the registered schema;
4. deserialize consumer values using the same schema contract;
5. evolve schemas using explicit compatibility rules.

For a new implementation, use the current Confluent serializer/schema-registry APIs rather than copying the deprecated `AvroProducer` / `AvroConsumer` example verbatim.

## Event design lesson

The original click-event exercise modelled nested attributes as an Avro map containing record values. That remains a useful example of why event contracts should be designed deliberately before producers and consumers are coupled to them.

## Intentionally omitted

- generated CSV datasets;
- course/startup helper scripts;
- deprecated Avro client APIs;
- infinite-loop demo code;
- swallowed exceptions during topic creation.
