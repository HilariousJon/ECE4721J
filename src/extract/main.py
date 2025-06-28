import json
from fastavro import reader

def extract_avro_to_json(avro_file_path, output_json_path):
    with open(avro_file_path, 'rb') as f_in, open(output_json_path, 'w', encoding='utf-8') as f_out:
        avro_reader = reader(f_in)

        for record in avro_reader:
            track_id = record.get("track_id")

            analysis = record.get("analysis", {})
            duration = analysis.get("duration")
            tempo = analysis.get("tempo")
            segments_pitches = analysis.get("segments_pitches")  # 2D array

            metadata = record.get("metadata", {})
            artist_name = metadata.get("artist_name")
            title = metadata.get("title")
            year = metadata.get("year")

            json_record = {
                "track_id": track_id,
                "title": title,
                "artist_name": artist_name,
                "year": year,
                "duration": duration,
                "tempo": tempo,
                "segments_pitches": segments_pitches
            }

            json.dump(json_record, f_out)
            f_out.write('\n')

if __name__ == "__main__":
    extract_avro_to_json("your_file.avro", "output.json")