import json
import sys
from time import sleep
from kafka import KafkaProducer
import pandas as pd

characterCount = 113

if __name__ == "__main__":

    # keys = [
    #     "team_win",
    #     "cluster_id"
    #     "game_mode",
    #     "game_type",
    #     *[f"character_{x}" for x in range(characterCount+1)]
    # ]
    empty_entry = {
        "team_win" : 0,
        "cluster_id" : 0,
        "game_mode" : 0,
        "game_type" : 0,
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
        while True:
            team_1_heroes = [1, 2, 3, 4, 5]
            team_2_heroes = [6, 7, 8, 9, 10]
            
            row = empty_entry.copy()
            
            for hero_id in team_1_heroes:
                row[f"character_{hero_id}"] = 1
            for hero_id in team_2_heroes:
                row[f"character_{hero_id}"] = -1
                
            producer.send("topicBD", row)
            
            # print(row)
            print("sent data: ")
            print(f"{team_1_heroes=}")
            print(f"{team_2_heroes=}")
            
            sleep(3)
    except KeyboardInterrupt:
        producer.close()
        pass
