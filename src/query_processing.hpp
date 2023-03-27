#ifndef QUERY_PROCESS_H
#define QUERY_PROCESS_H

#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <algorithm>

#include "block.hpp"
#include "constants.hpp"

class QueryProcessor
{
private:
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
    void process_query(uint16_t year1, uint16_t year2, bool city);
    void output_to_year_pos_file(Block<ColumnTypeConstants::position> &year_position_block, int &write_pos, int &pos_index);
    void get_year_pos(int year1, int year2);
    void print_year_pos();
    std::vector<int> get_month_pos(int month, std::string year_pos_file);
};

#endif