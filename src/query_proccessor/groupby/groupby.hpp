#if !defined(GROUPBY_H)
#define GROUPBY_H

#include "../aggregation/aggregate_function.hpp"
#include "../aggregation/max_aggregation.hpp"
#include "../aggregation/min_aggregation.hpp"

#include <fstream>
#include <map>

struct GroupByYearMonthPosition
{
    uint32_t position;
    char key[8];
    
    GroupByYearMonthPosition(){}
    
    GroupByYearMonthPosition(uint32_t position, uint16_t year, uint8_t month)
    {
        this->position = position;
        std::string year_string = std::to_string(year);
        std::string month_string = std::to_string(month);
        if (month_string.size() == 1) month_string = "0" + month_string;
        for (int i=0; i<year_string.size(); ++i)
        {
           this->key[i] = year_string[i];
        }
        this->key[4] = '-';
        for (int i=0; i<month_string.size(); ++i)
        {
            this->key[5+i] = month_string[i];
        }
        this->key[7] = '\0';
    }
};


// Non-Generic Group By

class GroupBy
{
private:
    std::ifstream position_input_file;
    std::ofstream position_key_output_file;
    std::ifstream data_file;
public:
    GroupBy();
    ~GroupBy();
    void save_groupby_key(std::string position_input_file_name, std::string position_key_output_file_name, std::string data_file_name);
    void save_aggregation(std::string position_input_file_name, std::string max_file_name, std::string min_file_name, std::string data_file_name, std::map<std::string, float>& aggr_min, std::map<std::string, float>& aggr_max);
};

GroupBy::GroupBy()
{
}


void GroupBy::save_groupby_key(std::string position_input_file_name, std::string position_key_output_file_name, std::string data_file_name)
{
    this->position_input_file.open(position_input_file_name, std::ios::binary);
    this->position_key_output_file.open(position_key_output_file_name, std::ios::binary);
    this->data_file.open(data_file_name, std::ios::binary);

    int position_block_size = 2048;
    std::vector<uint32_t> positions(position_block_size / sizeof(uint32_t));
    Block<time_t> data_block = Block<time_t>(2048);

    std::vector<GroupByYearMonthPosition> qualififed_positions_with_key;
    int num_qualified_tuples = 0;

    while(this->position_input_file.good())
    {
        this->position_input_file.read(reinterpret_cast<char *>(positions.data()), positions.size() * sizeof(uint32_t));

        for (int i=0; i<positions.size(); ++i)
        {
            data_block.read_data(this->data_file, positions[i], false);
            std::vector<time_t> data = data_block.get_data();
            std::pair<int, int> range = data_block.get_range();

            int index = positions[i] - range.first;

            time_t value = data[index];
            tm *ltm = localtime(&value);

            GroupByYearMonthPosition key = GroupByYearMonthPosition(positions[i], 1900 + ltm->tm_year, 1 + ltm->tm_mon);
            ++num_qualified_tuples;
            qualififed_positions_with_key.push_back(key);

            if(num_qualified_tuples >= position_block_size / sizeof(GroupByYearMonthPosition))
            {
                this->position_key_output_file.write(reinterpret_cast<char *>(qualififed_positions_with_key.data()), qualififed_positions_with_key.size() * sizeof(GroupByYearMonthPosition));
                num_qualified_tuples = 0;
                qualififed_positions_with_key.clear();
            }
        }
    }

    if(num_qualified_tuples > 0)
    {
        this->position_key_output_file.write(reinterpret_cast<char *>(qualififed_positions_with_key.data()), qualififed_positions_with_key.size() * sizeof(GroupByYearMonthPosition));
        num_qualified_tuples = 0;
        qualififed_positions_with_key.clear();
    }

    this->position_input_file.close();
    this->position_key_output_file.close();
    this->data_file.close();
}

// For temp and humidity
void GroupBy::save_aggregation(std::string position_input_file_name, std::string max_file_name, std::string min_file_name, std::string data_file_name, std::map<std::string, float>& aggr_min, std::map<std::string, float>& aggr_max)
{
    this->position_input_file.open(position_input_file_name, std::ios::binary);
    this->data_file.open(data_file_name, std::ios::binary);

    int position_block_size = 2048;
    std::vector<GroupByYearMonthPosition> positions(position_block_size / sizeof(GroupByYearMonthPosition));

    Block<float> data_block = Block<float>(2048);

    std::map<std::string, MaxAggregation<float>> max_aggr;
    std::map<std::string, MinAggregation<float>> min_aggr;

    while (this->position_input_file.good())
    {
        this->position_input_file.read(reinterpret_cast<char *>(positions.data()), positions.size() * sizeof(GroupByYearMonthPosition));

        for (int i=0; i<positions.size(); ++i)
        {
            data_block.read_data(this->data_file, positions[i].position, false);
            std::vector<float> data = data_block.get_data();
            std::pair<int, int> range = data_block.get_range();

            int index = positions[i].position - range.first;

            float value = data[index];
            
            if (value == -1) continue; // we encode -1 for missing values

            std::string key(positions[i].key);

            if (max_aggr.find(key) == max_aggr.end())
            {
                max_aggr[key] = MaxAggregation<float>();
            }
            if (min_aggr.find(key) == min_aggr.end())
            {
                min_aggr[key] = MinAggregation<float>();
            }

            max_aggr[key].add_value(value);
            min_aggr[key].add_value(value);
        }
    }

    this->position_input_file.close();
    this->position_key_output_file.close();
    this->data_file.close();

    std::map<std::string, Predicate<float>*> min_compare;
    std::map<std::string, Predicate<float>*> max_compare;

    for (auto it=max_aggr.begin(); it!=max_aggr.end(); ++it)
    {
        aggr_max[it->first] = it->second.compute_value();
        max_compare[it->first] = new AtomicPredicate("=", it->second.compute_value());
    }

    for (auto it=min_aggr.begin(); it!=min_aggr.end(); ++it)
    {
        aggr_min[it->first] = it->second.compute_value();
        min_compare[it->first] = new AtomicPredicate("=", it->second.compute_value());
    }

    this->position_input_file.open(position_input_file_name, std::ios::binary);
    this->data_file.open(data_file_name, std::ios::binary);

    std::ofstream max_output_pos;
    std::ofstream min_output_pos; 

    max_output_pos.open(max_file_name, std::ios::binary);
    min_output_pos.open(min_file_name, std::ios::binary);

    std::vector<uint32_t> qualififed_positions_min;
    int num_qualified_tuples_min = 0;

    std::vector<uint32_t> qualififed_positions_max;
    int num_qualified_tuples_max = 0;

    while (this->position_input_file.good())
    {
        this->position_input_file.read(reinterpret_cast<char *>(positions.data()), positions.size() * sizeof(GroupByYearMonthPosition));

        for (int i=0; i<positions.size(); ++i)
        {
            data_block.read_data(this->data_file, positions[i].position, false);
            std::vector<float> data = data_block.get_data();
            std::pair<int, int> range = data_block.get_range();

            int index = positions[i].position - range.first;

            float value = data[index];
            
            if (value == -1) continue; // we encode -1 for missing values

            std::string key(positions[i].key);

            if (min_compare[key]->evaluate_expr(value))
            {
                qualififed_positions_min.push_back(positions[i].position);
                num_qualified_tuples_min++;
            }
            if (max_compare[key]->evaluate_expr(value))
            {
                qualififed_positions_max.push_back(positions[i].position);
                num_qualified_tuples_max++;
            }

            if(num_qualified_tuples_min >= position_block_size / sizeof(uint32_t))
            {
                min_output_pos.write(reinterpret_cast<char *>(qualififed_positions_min.data()), qualififed_positions_min.size() * sizeof(uint32_t));
                num_qualified_tuples_min = 0;
                qualififed_positions_min.clear();
            }

            if(num_qualified_tuples_max >= position_block_size / sizeof(uint32_t))
            {
                max_output_pos.write(reinterpret_cast<char *>(qualififed_positions_max.data()), qualififed_positions_max.size() * sizeof(uint32_t));
                num_qualified_tuples_max = 0;
                qualififed_positions_max.clear();
            }
        }
    }

    if(num_qualified_tuples_max > 0)
    {
        max_output_pos.write(reinterpret_cast<char *>(qualififed_positions_max.data()), qualififed_positions_max.size() * sizeof(uint32_t));
        num_qualified_tuples_max = 0;
        qualififed_positions_max.clear();
    }
    if(num_qualified_tuples_min > 0)
    {
        min_output_pos.write(reinterpret_cast<char *>(qualififed_positions_min.data()), qualififed_positions_min.size() * sizeof(uint32_t));
        num_qualified_tuples_min = 0;
        qualififed_positions_min.clear();
    }

    this->data_file.close();
    this->position_input_file.close();
    min_output_pos.close();
    max_output_pos.close();
}

GroupBy::~GroupBy()
{
    this->position_input_file.close();
    this->position_key_output_file.close();
    this->data_file.close();
}


#endif // GROUPBY_H
