# ECE4721 lab2

## Team members

- Li Yanzhi
- Zuo Tianyou
- Zhang Lingwei
- Aghamatlab Arkbarzade

## Contribution

- Zhang Lingwei: ex2.1, ex2.2, ex2.3
- Li Yanzhi: ex2.4
- Zuo Tianyou: ex2.5

## Data generation

Scripts `generator.sh` generates CSV file contains random names, studentsID and grades.

1; prepare a two files in a folder `inputs` in the same directory, put two files in `inputs` folder in the following format.

<details> <summary> <code>inputs/firstnames.txt</code> </summary>

```txt
Aaron
Aaron
Abbey
Abbie
Abby
Abdul
Abe
Abel
Abigail
Abraham
Abram
Ada
Adah
Adalberto
Adaline
```

</details>

<details> <summary> <code>inputs/firstnames.txt</code> </summary>

```txt
Aaberg
Aaby
Aadland
Aagaard
Aakre
Aaland
Aalbers
Aalderink
Aalund
Aamodt
Aamot
Aanderud
Aanenson
Aanerud
```

</details>

2; run the following command:

```shell
bash generator.sh <NUM_OF_STUDENTS>
```

3; get the data in `./outputs/students_<NUM_OF_STUDENTS>.csv`, following is a sample output

<details> <summary> <code> ./outputs/students_<NUM_OF_STUDENTS>.csv </code> </summary>

```csv
Name,StudentID,Grade
Oda Caponi,1000024123,14
Rickey Brochhausen,1000031953,50
Emerita Casarella,1000022641,89
Shawnta Hamann,1000005753,23
Jong Friesner,1000030019,27
Bryon Bruen,1000001455,55
Laci Condi,1000011191,65
Georgine Gyatso,1000007034,57
Cyril Devich,1000004324,82
Dewitt Banales,1000023598,41
Cameron Christensen,1000010229,6
Armando Gramble,1000012553,3
```

</details>

## Mapper

- `mapper.sh` reads from standard input, with the format the same as `./students.csv`, and then returns the tab-separated pairs in the format as: `studentID<TAB>grade`
- run

```shell
bash mapper.sh < ../outputs/students_<NUM_OF_STUDENTS>.csv
```

<details> <summary> output sample </summary>

```shell
1000031595      22
1000000136      76
1000000421      45
1000021595      39
1000023572      86
1000026691      41
1000009388      9
1000021124      1
1000010311      38
```

</details>

## Reducer

- Reducer reads tab-separated pairs from the standard input, each of which is composed of a studentID and a grade, and returns the max grade for each student on the standard output.
- run

```shell
bash mapper.sh < ./outputs/students_<NUM_OF_STUDENTS>.csv | bash reducer.sh
```

<details> <summary> output sample </summary>

```shell
1000032135      17
1000001455      91
1000005753      83
1000007034      80
1000011191      65
1000022641      89
1000024123      55
1000030019      57
1000031953      85
```

</details>

## Hadoop streamming tasks (Single node / Clusters)

1; make sure start hadoop as single node status, run

```shell
start-all.sh
```

2; copy the `students.csv` files to `HDFS`, run

```shell
hdfs dfs -mkdir -p /user/hadoopuser/input
hdfs dfs -put students.csv /user/hadoopuser/input/
hdfs dfs -ls
```

3; run single node mapreduce on hadoop streaming, run

```shell
make stream
```

<details> <summary> specific command of <code>make stream</code> </summary>

```shell
make clean
hadoop jar /home/hadoopuser/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar \
    -input /user/hadoopuser/input/students.csv \
    -output /user/hadoopuser/output \
    -mapper mapper.sh \
    -reducer reducer.sh \
    -file mapper.sh \
    -file reducer.sh
```

namely:

```shell
hadoop jar <HADOOP_HOME>/share/hadoop/tools/lib/hadoop-streaming-3.3.2.jar \
    -input <DFS_INPUT_DIR> \
    -output <DFS_OUTPUT_DIR> \
    -mapper <MAPPER> \
    -reducer <REDUCER> \
    -file <LOCAL_MAPPER_DIR> \
    -file <LOCAL_REDUCER_DIR>
```

</details>

4; get the output from hadoop streamming, run

```shell
hdfs dfs -cat /user/hadoopuser/output/part-00000
```

<details> <summary> sample outputs </summary>

```shell
1000004581      56
1000005084      46
1000000318      34
1000000853      63
1000001447      31
1000001994      7
1000002844      47
1000002930      4
1000003026      5
1000003650      56
1000003689      37
1000004298      54
1000004429      5
```

</details>

or you can check via `Utilities->Browse` file system in `localhost:9870` to check the directory and files on the `HDFS`.

## Benchmarking results

### How to run

- when you have single node, run

```shell
make benchmark-single
```

- when you have clusters, run

```shell
make benchmark-cluster
```

- after you run this, you will have a log csv file generated in `./log/metrics.csv`, with sample content as follow, note when status `1` represent single node, and status `2` represent cluster

```csv
num,size,time,status
1000,28694,11.820346525,1
10000,287055,12.510691142,1
100000,2870662,12.846154963,1
1000000,28703670,13.427003886,1
```

- then run the following to get testing images results

```shell
make plot
```
