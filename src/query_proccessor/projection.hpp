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
        if (this->month.size() == 1) this->month = "0" + this->month;
        this->day = std::to_string(day);
        if (this->day.size() == 1) this->day = "0" + this->day;

        this->date = this->year + "-" + this->month + "-" + this->day;
    }
};

class Projection
{
private:
    /* data */
public:
    Projection();

    void save_result(std::string position_file_name, std::ofstream &output_file, std::string data_file_name ,std::string station, std::string category, std::map<std::string, float> value);
};

Projection::Projection()
{
}


void Projection::save_result(std::string position_file_name, std::ofstream &output_file, std::string data_file_name ,std::string station, std::string category, std::map<std::string, float> value)
{
    std::ifstream position_file(position_file_name, std::ios::binary);
    std::ifstream data_file(data_file_name, std::ios::binary);

    int position_block_size = 2048;
    std::vector<uint32_t> positions(position_block_size / sizeof(uint32_t));
    Block<time_t> data_block = Block<time_t>(2048);

    std::map<std::string, std::set<std::string>> dates;

    for (auto it=value.begin(); it!=value.end(); ++it)
    {
        dates[it->first] = {};
    }

    int pos = 0;

    while(position_file.good())
    {
        position_file.read(reinterpret_cast<char *>(positions.data()), positions.size() * sizeof(uint32_t));

        for (int i=0; i<positions.size(); ++i)
        {
            if (positions[i] < pos) continue;
            else pos = positions[i];
            data_block.read_data(data_file, positions[i], false);
            std::vector<time_t> data = data_block.get_data();
            std::pair<int, int> range = data_block.get_range();

            int index = positions[i] - range.first;

            time_t value = data[index];
            tm *ltm = localtime(&value);

            Date date_object = Date(1900 + ltm->tm_year, 1 + ltm->tm_mon, ltm->tm_mday);

            std::string date_string = date_object.date;

            std::string key = date_object.year + "-" + date_object.month;

            dates[key].insert(date_string);

        }
    }

    for (auto it=dates.begin(); it!=dates.end(); ++it)
    {
        for (auto item=it->second.begin(); item!=it->second.end(); ++item)
        {
            output_file << *item << "," << station << "," << category <<',' << value[it->first] << '\n'; 
        }
    }
    position_file.close();
    data_file.close();
}




#endif // PROJECTION_H
