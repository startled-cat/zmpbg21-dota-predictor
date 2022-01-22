import json
import sys
from kafka import KafkaProducer

characterCount = 113

LABEL_TOPIC = "topicBD2"

if __name__ == "__main__":

    empty_entry = {
        "team_win": 0,
        "cluster_id": 0,
        "game_mode": 0,
        "game_type": 0,
    }
    for character_id in range(1, characterCount+1):
        empty_entry[f"character_{character_id}"] = 0

    server = sys.argv[1] if len(sys.argv) == 2 else "localhost:9092"
    print(f"creating KafkaProducer {server}... ")
    producer = KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        api_version=(2, 7, 0),
    )

    try:
        sample_teams = [[1, 2, 3, 4, 5], [16, 17, 18, 19, 110]]
        i = True
        while True:
            print("press enter to send")
            sth = input()
            team_1_heroes = sample_teams[0 if i else 1]
            team_2_heroes = sample_teams[0 if not i else 1]

            row = empty_entry.copy()

            for hero_id in team_1_heroes:
                row[f"character_{hero_id}"] = 1

            for hero_id in team_2_heroes:
                row[f"character_{hero_id}"] = -1

            producer.send("topicBD", row)

            print(f"sent data: {team_1_heroes=} ; {team_2_heroes=}")
            i = not i
    except KeyboardInterrupt:
        producer.close()
