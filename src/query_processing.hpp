/**
 * @file query_processing.hpp
 * @author Atul, Siddharth, Srishti
 * @brief Query processing header file
 * @version 0.1
 * @date 2023-04-20
 *
 * @copyright Copyright (c) 2023
 *
 */

#ifndef QUERY_PROCESS_H
#define QUERY_PROCESS_H

#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <algorithm>

#include "block.hpp"
#include "constants.hpp"

/**
 * @brief Query Processor abstraction class
 * This class provides an abstraction for a very generic query processor with extensible design to accommodate several different kinds of queries.
 */
class QueryProcessor
{
private:
    /**
     * @property block_size: desired block size
     * @property year_block: block to read year column
     * @property month_block: block to read month column
     * @property day_block: block to read day column
     * @property time_block: block to read time column
     * @property raw_timestamp_block: block to read raw timestamp column
     * @property city_block: block to read city column
     * @property temperature_block: block to read temperature column
     * @property humidity_block: block to read humidity column
     */
    int block_size;
    Block<ColumnTypeConstants::year> year_block;
    Block<ColumnTypeConstants::month> month_block;
    Block<ColumnTypeConstants::day> day_block;

    Block<ColumnTypeConstants::time> time_block;

    Block<ColumnTypeConstants::raw_timestamp> raw_timestamp_block;

    Block<ColumnTypeConstants::city> city_block;

    Block<ColumnTypeConstants::temperature> temperature_block;
    Block<ColumnTypeConstants::humidity> humidity_block;

public:
    QueryProcessor(int block_size);
    void process_query(std::string matric_num, uint16_t year1, uint16_t year2, bool city);
    void output_to_year_pos_file(Block<ColumnTypeConstants::position> &year_position_block, int &write_pos, int &pos_index);
    void get_year_pos(int year1, int year2);
    void print_year_pos();
    std::vector<int> get_month_pos(int month, std::string year_pos_file);
    void experiment(std::string matric_num, uint16_t year1, uint16_t year2, bool city, std::string yearFilterType);

};

#endif