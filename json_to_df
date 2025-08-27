#É para tranformar o json selecionado em dataframe 

import json
import pandas as pd

#features que achei mais relevante 
def extrair_features(events):
    features = {
        "login_failed": 0,
        "login_success": 0,
        "commands_count": 0,
        "connect_count": 0,
        "total_duration": 0
    }

    for evento in events:
        eventid = evento.get("eventid", "")

        if eventid == "cowrie.login.failed":
            features["login_failed"] += 1
        elif eventid == "cowrie.login.success":
            features["login_success"] += 1
        elif eventid == "cowrie.command.input":
            features["commands_count"] += 1
        elif eventid == "cowrie.session.connect":
            features["connect_count"] += 1
        elif eventid == "cowrie.session.closed":
            features["total_duration"] += evento.get("duration", 0)

    return features

#abri somente um arquivo para teste
with open("/content/cowrie_2019-05-14.json", "r") as f:
    data = json.load(f)

all_sessions = []

for session_block in data:
    for session_key, events in session_block.items():
        for event in events:
            event["session_key"] = session_key
            all_sessions.append(event)

df = pd.DataFrame(all_sessions)

sessions_features = []

for session_id, group in df.groupby("session_key"):
    features = extrair_features(group.to_dict("records"))
    features["session_id"] = session_id
    sessions_features.append(features)

df_features = pd.DataFrame(sessions_features)

print(df_features)
