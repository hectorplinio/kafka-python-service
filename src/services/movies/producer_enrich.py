import json

from confluent_kafka import Consumer, Producer


class Enricher:
    def __init__(self):
        consumer_config = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "new_enricher_group2",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        producer_config = {"bootstrap.servers": "localhost:9092"}
        self.consumer_movies = Consumer(consumer_config)
        self.consumer_actors = Consumer(consumer_config)
        self.producer = Producer(producer_config)
        self.movies = {}
        self.actors = {}
        self.is_running = True

    def consume_all_movies(self):
        self.consumer_movies.subscribe(["movies_topic"])
        print("Subscribed to movies_topic")
        empty_poll_count = 0
        try:
            while self.is_running and empty_poll_count < 10:
                msg = self.consumer_movies.poll(timeout=1.0)
                if msg is None:
                    empty_poll_count += 1
                    print("No movie to consume, empty poll count:", empty_poll_count)
                    continue
                if msg.error():
                    print(f"Movie consumer error: {msg.error()}")
                    continue
                try:
                    movie = json.loads(msg.value())
                    movie_title = movie["title"]
                    self.movies[movie_title] = movie
                    empty_poll_count = 0
                    print(f"Consumed movie: {movie_title}")
                except json.JSONDecodeError as e:
                    print(f"Error decoding movie message: {e}")
        except KeyboardInterrupt:
            self.is_running = False
        finally:
            self.consumer_movies.close()

    def consume_all_actors(self):
        self.consumer_actors.subscribe(["actors_topic"])
        print("Subscribed to actors_topic")
        empty_poll_count = 0
        try:
            while self.is_running and empty_poll_count < 10:
                msg = self.consumer_actors.poll(timeout=1.0)
                if msg is None:
                    empty_poll_count += 1
                    print("No actor to consume, empty poll count:", empty_poll_count)
                    continue
                if msg.error():
                    print(f"Actor consumer error: {msg.error()}")
                    continue
                try:
                    actor = json.loads(msg.value())
                    for movie_title in actor["movies"]:
                        if movie_title not in self.actors:
                            self.actors[movie_title] = []
                        self.actors[movie_title].append(actor["actor"])
                        empty_poll_count = 0
                        print(f"Added actor {actor['actor']} to movie {movie_title}")
                except json.JSONDecodeError as e:
                    print(f"Error decoding actor message: {e}")
        except KeyboardInterrupt:
            self.is_running = False
        finally:
            self.consumer_actors.close()

    def enrich_and_produce_movies(self):
        for movie_title, movie in self.movies.items():
            if movie_title in self.actors:
                movie["actors"] = self.actors[movie_title]
            else:
                movie["actors"] = []
            self.produce_enriched_movie(movie)

    def produce_enriched_movie(self, movie):
        value = json.dumps(movie)
        self.producer.produce("enriched_movies_topic", value=value)
        self.producer.flush()
        print(f"Produced enriched movie: {movie['title']}")

    def run(self):
        print("Starting to consume movies...")
        self.consume_all_movies()
        print("Finished consuming movies.")
        print("Starting to consume actors...")
        self.consume_all_actors()
        print("Finished consuming actors.")
        print("Starting to enrich and produce movies...")
        self.enrich_and_produce_movies()
        print("Finished enriching and producing movies.")


if __name__ == "__main__":
    enricher = Enricher()
    enricher.run()
