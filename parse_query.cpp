#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <cstring>
#include <cassert>

#include "src/preprocessing.hpp"
#include "src/query.hpp"
#include "src/block.hpp"

Query parse_query(std::string matric_no)
{
    int year = (int)(matric_no[matric_no.size() - 2]) - 48;

    bool city = ((int)matric_no[matric_no.size() - 2] - 48) % 2;

    return Query(year, city);
}

int main(int argc, char **argv)
{
    try
    {
        assert(argc == 2);

        std::string matriculation_number = argv[1];

        Query query = parse_query(matriculation_number);
        std::cout << "Query: " << query.year1 << " " << query.year2 << " " << query.city << std::endl;
        // preprocess_csv();
        int block_size = 8;
        int start_pos = 191528;

        Block<ColumnTypeConstants::year> year_block(block_size);
        Block<ColumnTypeConstants::month> month_block(block_size);
        Block<ColumnTypeConstants::day> day_block(block_size);

        Block<ColumnTypeConstants::time> time_block(block_size);

        Block<ColumnTypeConstants::city> city_block(block_size);

        Block<ColumnTypeConstants::temperature> temperature_block(block_size);
        Block<ColumnTypeConstants::humidity> humidity_block(block_size);

        std::ifstream fin;

        for (auto it = FileNameConstants::file_names.begin(); it != FileNameConstants::file_names.end(); ++it)
        {
            fin.open("data/column_store/" + it->second, std::ios::binary);
            std::cout << it->second << std::endl;
            if (it->second == "year_encoded.dat")
            {
                year_block.read_data(fin, start_pos);
                year_block.print_data();
            }
            else if (it->second == "month_encoded.dat")
            {
                month_block.read_data(fin, start_pos);
                month_block.print_data();
            }
            else if (it->second == "day_encoded.dat")
            {
                day_block.read_data(fin, start_pos);
                day_block.print_data();
            }
            else if (it->second == "time_encoded.dat")
            {
                time_block.read_data(fin, start_pos);
                time_block.print_data();
            }
            else if (it->second == "city_encoded.dat")
            {
                city_block.read_data(fin, start_pos);
                city_block.print_data();
            }
            else if (it->second == "temperature_encoded.dat")
            {
                temperature_block.read_data(fin, start_pos);
                temperature_block.print_data();
            }
            else if (it->second == "humidity_encoded.dat")
            {
                humidity_block.read_data(fin, start_pos);
                humidity_block.print_data();
            }
            fin.close();
        }

        // std::string month_filename = FileNameConstants::file_names.at("month");
        // fin.open("data/column_store/" + month_filename, std::ios::binary);
        // month_block.read_data(fin, 191528);
        // month_block.print_data();
    }

    catch (const std::exception &exc)
    {
        std::cerr << "Exception " << exc.what();
    }
    return 0;
}