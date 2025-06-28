import os
import h5py
import numpy as np
import fastavro
from fastavro.schema import load_schema, parse_schema


def concat_files():
    # Array that will hold all the records
    # Each record is a dictionary with the fields defined in the schema
    # The list of records will be written to an Avro file
    records = []

    # Iterate through the directory structure and find all .h5 files
    for root, _, files in os.walk('./data/msd_subset/MillionSongSubset'):
        for f in files:

            # Skip files that are not .h5
            if not f.endswith('.h5'):
                continue

            # Open the .h5 file and read the data
            path = os.path.join(root, f)
            with h5py.File(path, 'r') as h5f:
                song_compound     = h5f['analysis/songs'][()][0]
                metadata_compound = h5f['metadata/songs'][()][0]
                mb_compound       = h5f['musicbrainz/songs'][()][0]
                # Create a record dictionary with the fields defined in the schema
                rec = {
                    'track_id': song_compound['track_id'].decode(),

                    'analysis': {
                        'analysis_sample_rate': int(song_compound['analysis_sample_rate']),
                        'danceability':           float(song_compound['danceability']),
                        'duration':               float(song_compound['duration']),
                        'end_of_fade_in':         float(song_compound['end_of_fade_in']),
                        'energy':                 float(song_compound['energy']),
                        'loudness':               float(song_compound['loudness']),
                        'start_of_fade_out':      float(song_compound['start_of_fade_out']),
                        'tempo':                  float(song_compound['tempo']),
                        'time_signature':         int(song_compound['time_signature']),
                        'time_signature_confidence': float(song_compound['time_signature_confidence']),
                        'key':                    int(song_compound['key']),
                        'key_confidence':         float(song_compound['key_confidence']),
                        'mode':                   int(song_compound['mode']),
                        'mode_confidence':        float(song_compound['mode_confidence']),
                        'bars_start':     h5f['analysis/bars_start'][()].tolist(),
                        'bars_confidence':h5f['analysis/bars_confidence'][()].tolist(),
                        'beats_start':    h5f['analysis/beats_start'][()].tolist(),
                        'beats_confidence':h5f['analysis/beats_confidence'][()].tolist(),
                        'sections_start':    h5f['analysis/sections_start'][()].tolist(),
                        'sections_confidence':h5f['analysis/sections_confidence'][()].tolist(),
                        'tatums_start':      h5f['analysis/tatums_start'][()].tolist(),
                        'tatums_confidence': h5f['analysis/tatums_confidence'][()].tolist(),
                        'segments_start':    h5f['analysis/segments_start'][()].tolist(),
                        'segments_confidence':h5f['analysis/segments_confidence'][()].tolist(),
                        'segments_loudness_start': h5f['analysis/segments_loudness_start'][()].tolist(),
                        'segments_loudness_max':   h5f['analysis/segments_loudness_max'][()].tolist(),
                        'segments_loudness_max_time': h5f['analysis/segments_loudness_max_time'][()].tolist(),
                        'segments_pitches':  h5f['analysis/segments_pitches'][()].tolist(),
                        'segments_timbre':   h5f['analysis/segments_timbre'][()].tolist(),
                    },

                    'metadata': {
                        'artist_id':          metadata_compound['artist_id'].decode(),
                        'artist_name':        metadata_compound['artist_name'].decode() if metadata_compound['artist_name'] else None,
                        'artist_mbid':        metadata_compound['artist_mbid'].decode() if metadata_compound['artist_mbid'] else None,
                        'artist_familiarity': float(metadata_compound['artist_familiarity']),
                        'artist_hotttnesss':  float(metadata_compound['artist_hotttnesss']),
                        'artist_location':    metadata_compound['artist_location'].decode() if metadata_compound['artist_location'] else None,
                        'artist_latitude':    float(metadata_compound['artist_latitude']) if metadata_compound['artist_latitude'] else None,
                        'artist_longitude':   float(metadata_compound['artist_longitude']) if metadata_compound['artist_longitude'] else None,
                        'genre':              metadata_compound['genre'].decode() if metadata_compound['genre'] else None,
                        'artist_terms':       [t.decode() for t in h5f['metadata/artist_terms'][()]],
                        'artist_terms_freq':  h5f['metadata/artist_terms_freq'][()].tolist(),
                        'artist_terms_weight':h5f['metadata/artist_terms_weight'][()].tolist(),
                        'similar_artists':    [s.decode() for s in h5f['metadata/similar_artists'][()]],
                        'release':            metadata_compound['release'].decode() if metadata_compound['release'] else None,
                        'release_7digitalid': int(metadata_compound['release_7digitalid']),
                        'song_hotttnesss':    float(metadata_compound['song_hotttnesss']),
                        'song_id':            metadata_compound['song_id'].decode(),
                        'title':              metadata_compound['title'].decode() if metadata_compound['title'] else None,
                        'track_7digitalid':   int(metadata_compound['track_7digitalid']),
                        'year':               int(mb_compound['year']),
                    }
                }

                # Append the record to the list of records
                records.append(rec)
    return records

if __name__ == "__main__":
    try:
        # Concatenate all the .h5 files into a single list of records
        records = concat_files()
        schema = parse_schema(load_schema('Song.avsc'))
        out_path = 'output/songs.snappy.avro'  
        
        # Ensure the output directory exists
        os.makedirs('output', exist_ok=True)

        # Write the records to an Avro file with Snappy compression
        with open(out_path, 'wb') as fo:
            fastavro.writer(fo, schema, records, codec='snappy')

    except Exception as error:
        print(error)