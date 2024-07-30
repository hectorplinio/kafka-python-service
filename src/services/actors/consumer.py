from confluent_kafka import Consumer, KafkaException


class ActorConsumer:
    def __init__(self):
        self.consumer = Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": "actor_consumer",
                "auto.offset.reset": "smallest",
            }
        )

    def consume_actors(self, topic):
        self.consumer.subscribe(["actors_topic"])

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException:
                        continue
                    else:
                        print(f"Actor consumer error: {msg.error()}")
                        break

                print(
                    f"Consumed message from topic {topic}: {msg.key()}, {msg.value()}"
                )

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


if __name__ == "__main__":
    consumer = ActorConsumer()
    consumer.consume_actors("actors_topic")
