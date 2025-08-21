import os
import random
from pathlib import Path
from dataclasses import dataclass
from typing import List

# Constants
DATA_NUMBER = 20
ENTRY_PER_FILE = 20
CSV_FILE_COUNT = 20
BASE_DIR = Path("./data/small_generated_csv")

SAMPLE_FIRSTNAMES = [
    "Alice",
    "Bob",
    "Charlie",
    "Diana",
    "Eve",
    "Frank",
    "Grace",
    "Hank",
    "Ivy",
    "Jack",
    "Kara",
    "Leo",
    "Mia",
    "Nick",
    "Olivia",
    "Paul",
    "Quinn",
    "Rose",
    "Steve",
    "Tina",
]

SAMPLE_LASTNAMES = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Miller",
    "Davis",
    "Garcia",
    "Rodriguez",
    "Wilson",
    "Martinez",
    "Anderson",
    "Taylor",
    "Thomas",
    "Hernandez",
]


@dataclass
class Person:
    full_name: str
    id: int
    grade: int


def generate_people(num: int) -> List[Person]:
    people: List[Person] = []
    used_ids = set()

    while len(people) < num:
        first = random.choice(SAMPLE_FIRSTNAMES)
        last = random.choice(SAMPLE_LASTNAMES)
        full_name = f"{first} {last}"
        unique_id = random.randint(1000000000, 9999999999)
        if unique_id in used_ids:
            continue
        used_ids.add(unique_id)
        grade = random.randint(0, 100)
        people.append(Person(full_name, unique_id, grade))

    return people


def generate_csv_files(
    people: List[Person], count: int, entries_per_file: int, output_dir: Path
):
    output_dir.mkdir(parents=True, exist_ok=True)
    for i in range(count):
        filename = output_dir / f"random_{i}.csv"
        with open(filename, "w", encoding="utf-8") as f:
            for _ in range(entries_per_file):
                person = random.choice(people)
                f.write(f"{person.full_name},{person.id},{person.grade}\n")


def main():
    people = generate_people(DATA_NUMBER)
    generate_csv_files(people, CSV_FILE_COUNT, ENTRY_PER_FILE, BASE_DIR)
    print(f"Generated {CSV_FILE_COUNT} CSV files in: {BASE_DIR.resolve()}")


if __name__ == "__main__":
    main()
