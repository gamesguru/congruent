import json

events = []
with open("../../state-res-events.json") as f:
    events.extend(json.load(f))

with open(
    "/tmp/remote-dag-l2xV0sd51lraysuRcsWVECge4NULaH3g-ou95vgDgiM-v12-grin.hu-d1-10.jsonl"
) as f:
    for line in f:
        if line.strip():
            events.append(json.loads(line))

with open("../../state-res-events-combined.json", "w") as f:
    json.dump(events, f)
