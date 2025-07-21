from mrjob.job import MRJob
from itertools import combinations
from collections import defaultdict
import json
import math
import sys
import time

class MRBucketTopK(MRJob):
    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg('--topk', type=int, default=50)

    def mapper(self, _, line):
        parts = line.strip().split('\t', 1)
        if len(parts) != 2:
            return
        bucket = parts[0][:]
        inner_json_str = json.loads(parts[1][1:-1])
        artist_id = inner_json_str["artist_id"]
        vec = inner_json_str["vec"]
        yield bucket, (artist_id, vec)

    def reducer(self, bucket, records):
        records = list(records)
        if len(records) < 2:
            return

        graph = defaultdict(list)
        for (id1, vec1), (id2, vec2) in combinations(records, 2):
            dist = math.sqrt(sum((a - b) ** 2 for a, b in zip(vec1, vec2)))
            for a, b in [(id1, id2), (id2, id1)]:
                graph[a].append((b, dist))

        for aid, neighbors in graph.items():
            topk = sorted(neighbors, key=lambda x: x[1])[:self.options.topk]
            yield aid, json.dumps({
                "artist_id": aid,
                "neighbors": [nid for nid, _ in topk]
            })

if __name__ == '__main__':
    start_time = time.time()
    mr_job = MRBucketTopK(args=sys.argv[1:])
    with mr_job.make_runner() as runner:
        runner.run()
        for line in runner.cat_output():
            sys.stdout.write(line.decode('utf-8'))
    print("\nTop-K computation time:", round(time.time() - start_time, 3), "seconds", file=sys.stderr)