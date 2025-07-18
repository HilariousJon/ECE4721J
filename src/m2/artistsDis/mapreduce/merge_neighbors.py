import json
from collections import defaultdict
import argparse
import time

def merge_neighbors(input_path, output_path, topk):
    artist_to_neighbors = defaultdict(list)

    with open(input_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                aid = obj["artist_id"]
                artist_to_neighbors[aid].extend(obj["neighbors"])
            except Exception:
                continue

    final_graph = {}
    for aid, neighbors in artist_to_neighbors.items():
        counts = defaultdict(int)
        for nid in neighbors:
            counts[nid] += 1
        sorted_n = sorted(counts.items(), key=lambda x: -x[1])[:topk]
        final_graph[aid] = [nid for nid, _ in sorted_n]

    with open(output_path, 'w') as out:
        for aid in final_graph:
            out.write(json.dumps({
                "artist_id": aid,
                "neighbors": final_graph[aid]
            }) + '\n')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--topk", type=int, default=50)
    args = parser.parse_args()

    start_time = time.time()
    merge_neighbors(args.input, args.output, args.topk)
    end_time = time.time()
    print("Merging neighbors time:", end_time - start_time, "seconds")
