from mrjob.job import MRJob
import json
import numpy as np
import time
import sys

class MRLSHBuckets(MRJob):
    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg('--num-hash', type=int, default=10)
        self.add_passthru_arg('--seed', type=int, default=42)

    def mapper(self, _, line):
        try:
            data = json.loads(line)
            hot = float(data.get("hotttnesss", "nan"))
            fam = float(data.get("familiarity", "nan"))
            lat = float(data.get("lat", "nan"))
            lon = float(data.get("lon", "nan"))
            vec = np.array([hot, fam, lat, lon])
            if np.isnan(vec).any():
                return
            artist_id = data["artist_id"]
            if isinstance(artist_id, bytes):
                artist_id = artist_id.decode("utf-8")
        except Exception:
            return

        for i in range(self.options.num_hash):
            rand_vec = np.random.randn(len(vec))
            bucket = int(np.dot(vec, rand_vec) > 0)
            bucket_key = f"{i}_{bucket}"
            record = {
                "bucket": bucket_key,
                "artist_id": artist_id,
                "vec": vec.tolist()
            }
            yield None, json.dumps(record)

if __name__ == '__main__':
    start_time = time.time()

    mr_job = MRLSHBuckets(args=sys.argv[1:])
    with mr_job.make_runner() as runner:
        runner.run()
        for line in runner.cat_output():
            sys.stdout.write(line.decode('utf-8'))

    end_time = time.time()
    print("\nLSH hashing time:", round(end_time - start_time, 3), "seconds", file=sys.stderr)
