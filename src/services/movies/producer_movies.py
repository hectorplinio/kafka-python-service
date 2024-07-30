import json
import os
import socket

from confluent_kafka import Producer


def load_movies(file_path):
    with open(file_path, "r") as file:
        return json.load(file)


def producer_movies(movies, topic):
    producer = Producer(
        {
            "bootstrap.servers": "localhost:9092",
            "client.id": socket.gethostname(),
        }
    )

    for movie in movies:
        value = json.dumps(movie)
        producer.produce(topic, value)

    producer.flush()


def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, "movies.json")
    movies = load_movies(file_path)
    producer_movies(movies, "movies_topic")


if __name__ == "__main__":
    main()
