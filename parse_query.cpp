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
        assert(argc == 3);

        std::string matriculation_number = argv[1];
        int target_pos = atoi(argv[2]);

        Query query = parse_query(matriculation_number);
        std::cout << "Query: " << query.year1 << " " << query.year2 << " " << query.city << std::endl;
        preprocess_csv();
        int block_size = 64;

        Block<ColumnTypeConstants::year> year_block(block_size);
        Block<ColumnTypeConstants::month> month_block(block_size);
        Block<ColumnTypeConstants::day> day_block(block_size);

        Block<ColumnTypeConstants::time> time_block(block_size);

        Block<ColumnTypeConstants::raw_timestamp> raw_timestamp_block(block_size);

        Block<ColumnTypeConstants::city> city_block(block_size);

        Block<ColumnTypeConstants::temperature> temperature_block(block_size);
        Block<ColumnTypeConstants::humidity> humidity_block(block_size);

        std::ifstream fin;

        for (auto it = FileNameConstants::file_names.begin(); it != FileNameConstants::file_names.end(); ++it)
        {
            fin.open("data/column_store/" + it->second, std::ios::binary);
            // std::cout << it->second << std::endl;
            if (it->second == "year_encoded.dat")
            {
                std::cout << "Year: ";
                year_block.read_data(fin, target_pos);
                // year_block.print_data();
            }
            else if (it->second == "month_encoded.dat")
            {
                std::cout << "Month: ";
                month_block.read_data(fin, target_pos);
                // month_block.print_data();
            }
            else if (it->second == "day_encoded.dat")
            {
                std::cout << "Day: ";
                day_block.read_data(fin, target_pos);
                // day_block.print_data();
            }
            else if (it->second == "time_encoded.dat")
            {
                std::cout << "Time: ";
                time_block.read_data(fin, target_pos);
                // time_block.print_data();
            }
            else if (it->second == "raw_timestamp_encoded.dat")
            {
                std::cout << "Raw Timestamp: ";
                raw_timestamp_block.read_data(fin, target_pos);
                // raw_timestamp_block.print_data();
            }
            else if (it->second == "city_encoded.dat")
            {
                std::cout << "City: ";
                city_block.read_data(fin, target_pos);
                // city_block.print_data();
            }
            else if (it->second == "temperature_encoded.dat")
            {
                std::cout << "Temperature: ";
                temperature_block.read_data(fin, target_pos);
                // temperature_block.print_data();
            }
            else if (it->second == "humidity_encoded.dat")
            {
                std::cout << "Humidity: ";
                humidity_block.read_data(fin, target_pos);
                // humidity_block.print_data();
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