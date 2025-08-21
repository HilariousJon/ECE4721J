#!/bin/bash

# pipeline: ./mapper.sh < students.csv | ./reducer.sh

awk -F'\t' '
{
    split($2, grades, " ")
    for (i in grades) {
        g = grades[i] + 0
        if (g >= max[$1]) {
            max[$1] = g
        }
    }
}
END {
    for (id in max) {
        print id "\t" max[id]
    }
}
'