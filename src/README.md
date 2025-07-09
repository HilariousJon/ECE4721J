# ECE4721J SU25 Project

## Preparation

- Make sure you run the program on Linux and use Python3.  
- Clone this repository:
```bash
git clone ssh://git@focs.ji.sjtu.edu.cn:2222/ece472-25su/p1team02.git
```
- Run the program under `p1team02/` rather than `p1team02/src/`.
- Create a virtual environment of python and install dependencies:
```bash
make init_env
```

## Usage

- Mount the data on the server every time you reset your computer:
```bash
make mount_data_init
```
- Unmount the data:
```bash
make unmount_data
```

- Convert HDF5 files from the MSD into Avro format and save the output to the `./data/` directory:
```bash
make aggregate_avro
```

- Extract the HDF5 files from the Avro format file:
```bash
make extract
```

- Run the spark job to find the distance between two artists:
```bash
make artists_dis_spark
```
