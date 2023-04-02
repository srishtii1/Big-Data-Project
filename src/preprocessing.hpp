#ifndef PRE_PROCESS_H
#define PRE_PROCESS_H

#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <cstdio>
#include <unordered_map>
#include <iomanip>
#include <locale>


#include "constants.hpp"

void preprocess_csv();
void createZonemap(int block_size);
void readZonemap(int block_size);

#endif