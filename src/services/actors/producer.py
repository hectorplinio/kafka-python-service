import json
import os
import socket

from confluent_kafka import Producer


def load_actors(file_path):
    with open(file_path, "r") as file:
        return json.load(file)


def emit_actors(actors, topic):
    producer = Producer(
        {
            "bootstrap.servers": "localhost:9092",
            "client.id": socket.gethostname(),
        }
    )

    for movie in actors:
        value = json.dumps(movie)
        producer.produce(topic, value)

    producer.flush()


def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, "actors.json")
    actors = load_actors(file_path)
    emit_actors(actors, "actors_topic")


if __name__ == "__main__":
    main()
