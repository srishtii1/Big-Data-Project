# CX4123_Project

## Steps:

1. Creating folders: To run our code, the following folders must be created:
   - data/ in root directory: Within this directory, the input CSV file "SingaporeWeather.csv" must be placed.
   - column/ in data/ directory: This directory can be left empty.
   - temp/ in column/ directory: This directory can be left empty.
   - output/ in data/ directory: This directory can be left empty.
   - zone_maps/ in data/ directory: This directory can be left empty.


   Alternatively you can run `setup.sh` or `setup.bat` depending on your system and download the file from https://drive.google.com/file/d/1dewplMwEdNJqMVOoyDOFpTPqGCURq4fL/view and copy it in `data/` directory.

2. Compilation: You must have the g++ compilation toolchain installed. Compilation can be done as follows form the root directory of the project:

```
g++ main.cpp src/*.cpp src/preprocessing/preprocessing.cpp -o main.exe
```

3.  Running: After compiling as above, the program can be run as:

```
.\main.exe U1923502C 1024
```

Note that in the above, U1923502C is the input matriculation number, and 1024 is the block size in bytes. The output of the query is saved in `./data/output/`

4. To run experiments with different filtering algorithms on year column, run the program as follows:

```
.\main.exe <MatriculationNumber> <BlockSize> exp
```
