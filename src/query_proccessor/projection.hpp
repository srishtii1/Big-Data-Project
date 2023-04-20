/**
 * @file projection.hpp
 * @author Atul
 * @brief Header file for projection class.
 * @version 0.1
 * @date 2023-04-20
 *
 * @copyright Copyright (c) 2023
 *
 */

#if !defined(PROJECTION_H)
#define PROJECTION_H

#include <fstream>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <set>
#include <vector>

#include "../block.hpp"



struct Date
{
    std::string year;
    std::string month;
    std::string day;

    std::string date;

    Date(uint16_t year, uint8_t month, uint8_t day)
    {
        this->year = std::to_string(year);
        this->month = std::to_string(month);
        if (this->month.size() == 1)
            this->month = "0" + this->month;
        this->day = std::to_string(day);
        if (this->day.size() == 1)
            this->day = "0" + this->day;

        this->date = this->year + "-" + this->month + "-" + this->day;
    }
};

class Projection
{
private:
    /* data */
    int block_size;
public:
    Projection(int block_size);

    void save_result(std::string position_file_name, std::ofstream &output_file, std::string data_file_name, std::string station, std::string category, std::map<std::string, float> value);
};

Projection::Projection(int block_size)
{
    this->block_size = block_size;
}

void Projection::save_result(std::string position_file_name, std::ofstream &output_file, std::string data_file_name, std::string station, std::string category, std::map<std::string, float> value)
{
    std::ifstream position_file(position_file_name, std::ios::binary);
    std::ifstream data_file(data_file_name, std::ios::binary);

    Block<ColumnTypeConstants::position> positions_block(this->block_size);
    std::vector<uint32_t> positions(this->block_size / sizeof(uint32_t));
    Block<time_t> data_block = Block<time_t>(this->block_size);

    std::map<std::string, std::set<std::string>> dates;

    for (auto it = value.begin(); it != value.end(); ++it)
    {
        dates[it->first] = {};
    }

    int pos = 0;

    while (position_file.good())
    {
        // position_file.read(reinterpret_cast<char *>(positions.data()), positions.size() * sizeof(uint32_t));
        bool status = positions_block.read_next_block(position_file);

        for (int i = 0; i < positions_block.get_data().size(); ++i)
        {
            if (positions_block.get_data()[i] < pos)
                continue;
            else
                pos = positions_block.get_data()[i];
            data_block.read_data(data_file, positions_block.get_data()[i], false);
            std::vector<time_t> data = data_block.get_data();
            std::pair<int, int> range = data_block.get_range();

            int index = positions_block.get_data()[i] - range.first;

            time_t value = data[index];
            tm *ltm = localtime(&value);

            Date date_object = Date(1900 + ltm->tm_year, 1 + ltm->tm_mon, ltm->tm_mday);

            std::string date_string = date_object.date;

            std::string key = date_object.year + "-" + date_object.month;

            dates[key].insert(date_string);
        }
    }

    for (auto it = dates.begin(); it != dates.end(); ++it)
    {
        for (auto item = it->second.begin(); item != it->second.end(); ++item)
        {
            output_file << *item << "," << station << "," << category << ',' << value[it->first] << '\n';
        }
    }
    position_file.close();
    data_file.close();
}

#endif // PROJECTION_H
