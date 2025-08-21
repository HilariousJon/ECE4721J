#!/bin/bash

STUDENT_NUM=$1

mapfile -t firstnames < ../inputs/firstnames.txt
mapfile -t lastnames < ../inputs/lastnames.txt

mkdir -p "../outputs"

echo "Name,StudentID,Grade" > "../outputs/students_$STUDENT_NUM.csv"

declare -A student_ids

for i in $(seq 1 "$STUDENT_NUM"); do
    firstname="${firstnames[RANDOM % ${#firstnames[@]}]}"
    lastname="${lastnames[RANDOM % ${#lastnames[@]}]}"
    fullname="$firstname $lastname"

    if [[ -z "${student_ids[$fullname]}" ]]; then
        student_ids["$fullname"]=$((1000000000 + RANDOM % 9000000000))
    fi
    student_id="${student_ids[$fullname]}"

    grade=$((RANDOM % 101))

    echo "$fullname,$student_id,$grade" >> "../outputs/students_$STUDENT_NUM.csv"
done
