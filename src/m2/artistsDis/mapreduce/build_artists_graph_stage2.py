from mrjob.job import MRJob
from itertools import combinations
from collections import defaultdict
import json
import math
import time
import sys

class MRBucketNeighbor(MRJob):
    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg('--topk', type=int, default=50)

    def mapper(self, _, line):
        line = line.decode('utf-8') if isinstance(line, bytes) else line
        parts = line.strip().split("\t")
        if len(parts) != 2:
            return
        entry = json.loads(parts[1])

        bucket = entry["bucket"]
        artist_id = entry["artist_id"]
        if isinstance(artist_id, str) and artist_id.startswith("b'") and artist_id.endswith("'"):
            artist_id = artist_id[2:-1]

        vec = entry["vec"]
        yield bucket, (artist_id, vec)

    def reducer(self, key, values):
        artists = list(values)
        if len(artists) < 2:
            return

        sys.stderr.buffer.write(f"Reducer got bucket={key} with {len(artists)} artists\n".encode())

        graph = defaultdict(list)
        for (id1, vec1), (id2, vec2) in combinations(artists, 2):
            dist = math.sqrt(sum((a - b) ** 2 for a, b in zip(vec1, vec2)))
            for aid, bid in [(id1, id2), (id2, id1)]:
                graph[aid].append((bid, dist))

        for aid, neighbors in graph.items():
            topk = sorted(neighbors, key=lambda x: x[1])[:self.options.topk]
            yield aid, json.dumps({
                "artist_id": aid,
                "neighbors": [nid for nid, _ in topk]
            })

if __name__ == '__main__':
    start_time = time.time()

    mr_job = MRBucketNeighbor(args=sys.argv[1:])
    with mr_job.make_runner() as runner:
        runner.run()
        for line in runner.cat_output():
            sys.stdout.write(line.decode('utf-8'))

    end_time = time.time()
    print("\nTop-K neighbors time:", round(end_time - start_time, 3), "seconds", file=sys.stderr)
