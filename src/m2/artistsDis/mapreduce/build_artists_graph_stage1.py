from mrjob.job import MRJob
import json
import numpy as np
import hashlib
import sys
import time

class MRLSHBuckets(MRJob):
    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg('--num-hash', type=int, default=20)  # total hash functions
        self.add_passthru_arg('--bands', type=int, default=5)
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
        except Exception:
            return

        np.random.seed(self.options.seed)
        num_hash = self.options.num_hash
        bands = self.options.bands
        rows_per_band = num_hash // bands
        rand_vectors = np.random.randn(num_hash, len(vec))

        # Step 1: generate signature vector
        sig = [int(np.dot(vec, r) > 0) for r in rand_vectors]

        # Step 2: group into bands
        for b in range(bands):
            start = b * rows_per_band
            end = start + rows_per_band
            band_sig = sig[start:end]
            band_str = ''.join(map(str, band_sig))
            band_hash = hashlib.md5(band_str.encode()).hexdigest()

            bucket_key = f"{b}_{band_hash}"
            yield bucket_key, json.dumps({
                "artist_id": artist_id,
                "vec": vec.tolist()
            })

if __name__ == '__main__':
    start = time.time()
    mr_job = MRLSHBuckets(args=sys.argv[1:])
    with mr_job.make_runner() as runner:
        runner.run()
        for line in runner.cat_output():
            sys.stdout.write(line.decode("utf-8"))
    print(f"\nLSH Hashing Time: {round(time.time() - start, 3)} seconds", file=sys.stderr)
