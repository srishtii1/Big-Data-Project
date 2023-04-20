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

/**
 * @file preprocessing.hpp
 * @author Atul, Srishti
 * @brief Header file for preprocessor
 * @version 0.1
 * @date 2023-04-20
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "../constants.hpp"

void preprocess_csv();
void createZonemap(int block_size);
void readZonemap(int block_size);

#endif