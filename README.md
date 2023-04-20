# CX4123_Project

## Steps:

1. Creating folders: To run our code, the following folders must be created:
   - data/ in root directory: Within this directory, the input CSV file "SingaporeWeather.csv" must be placed.
   - column/ in data/ directory: This directory can be left empty.
   - temp/ in column/ directory: This directory can be left empty.
   - output/ in data/ directory: This directory can be left empty.
   - zone_maps/ in data/ directory: This directory can be left empty.
2. Compilation: You must have the g++ compilation toolchain installed. Compilation can be done as follows form the root directory of the project:

```
g++ main.cpp src/\*.cpp -o main.exe
```

3.  Running: After cmopiling as above, the program can be run as:

```
.\main.exe U1923502C 64
```

Note that in the above, U1923502C is the input matriculation number, and 64 is block size.
