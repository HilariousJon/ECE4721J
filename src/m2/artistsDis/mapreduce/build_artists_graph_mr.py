import argparse
import time
import json
import math
from itertools import combinations
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRArtistGraph(MRJob):
    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg('--topk', type=int, default=5)
        self.add_file_arg('--input')
        self.add_passthru_arg('--output')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_collect,
                   reducer=self.reducer_compute_neighbors)
        ]

    def mapper_collect(self, _, line):
        data = json.loads(line)
        yield None, (data["artist_id"],
                     float(data["hotttnesss"]),
                     float(data["familiarity"]),
                     float(data["lat"]),
                     float(data["lon"]))

    def reducer_compute_neighbors(self, _, records):
        all_artists = list(records)
        distance_dict = {}
        for a1, a2 in combinations(all_artists, 2):
            id1, *vec1 = a1
            id2, *vec2 = a2
            dist = self.euclidean_distance(vec1, vec2)
            distance_dict.setdefault(id1, []).append((id2, dist))
            distance_dict.setdefault(id2, []).append((id1, dist))

        k = self.options.topk
        for aid in distance_dict:
            neighbors = sorted(distance_dict[aid], key=lambda x: x[1])[:k]
            neighbor_ids = [nid for nid, _ in neighbors]
            yield aid, neighbor_ids

    def euclidean_distance(self, v1, v2):
        return math.sqrt(sum((x - y) ** 2 for x, y in zip(v1, v2)))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--topk', type=int, default=50)
    args, unknown = parser.parse_known_args()

    start_time = time.time()

    mr_job = MRArtistGraph(args=unknown + ['--topk', str(args.topk), args.input])
    results = {}

    with mr_job.make_runner() as runner:
        runner.run()
        for key, value in mr_job.parse_output(runner.cat_output()):
            results[key] = value

    with open(args.output, 'w') as f:
        for artist_id in results:
            f.write(json.dumps({
                "artist_id": artist_id,
                "neighbors": results[artist_id]
            }) + '\n')

    end_time = time.time()

    print("Graph build time:", end_time - start_time, "seconds")