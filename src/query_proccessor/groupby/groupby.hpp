#if !defined(GROUPBY_H)
#define GROUPBY_H

#include "../aggregation/aggregate_function.hpp"
#include "../aggregation/max_aggregation.hpp"
#include "../aggregation/min_aggregation.hpp"
#include "../../block.hpp"
#include "groupbyyearmonthposition.hpp"

#include <fstream>
#include <map>

// Non-Generic Group By

class GroupBy
{
private:
    std::ifstream position_input_file;
    std::ofstream position_key_output_file;
    std::ifstream data_file;
    int block_size;

public:
    GroupBy();
    GroupBy(int block_size);
    ~GroupBy();
    void save_groupby_key(std::string position_input_file_name, std::string position_key_output_file_name, std::string data_file_name);
    void save_aggregation(std::string position_input_file_name, std::string max_file_name, std::string min_file_name, std::string data_file_name, std::map<std::string, float> &aggr_min, std::map<std::string, float> &aggr_max);
};

GroupBy::GroupBy()
{
}

GroupBy::GroupBy(int block_size)
{
    this->block_size = block_size;
}

void GroupBy::save_groupby_key(std::string position_input_file_name, std::string position_key_output_file_name, std::string data_file_name)
{
    this->position_input_file.open(position_input_file_name, std::ios::binary);
    this->position_key_output_file.open(position_key_output_file_name, std::ios::binary);
    this->data_file.open(data_file_name, std::ios::binary);

    int position_block_size = this->block_size;
    Block<ColumnTypeConstants::position> positions_block(position_block_size);
    // std::vector<ColumnTypeConstants::position> positions(position_block_size / ColumnSizeConstants::position);

    Block<ColumnTypeConstants::raw_timestamp> data_block = Block<ColumnTypeConstants::raw_timestamp>(this->block_size);

    // std::vector<GroupByYearMonthPosition> qualified_positions_with_key;
    Block<GroupByYearMonthPosition> qualified_positions_with_key_block(this->block_size);
    int num_qualified_tuples = 0;

    while (this->position_input_file.good())
    {
        // this->position_input_file.read(reinterpret_cast<char *>(positions.data()), positions.size() * ColumnSizeConstants::position);
        bool status = positions_block.read_next_block(this->position_input_file);

        for (int i = 0; i < positions_block.get_data().size(); ++i)
        {
            // std::cout << positions_block.block_data[i] << '\n';
            data_block.read_data(this->data_file, positions_block.block_data[i], false);
            std::vector<ColumnTypeConstants::raw_timestamp> data = data_block.get_data();
            if (data.size() == 0) break;
            std::pair<int, int> range = data_block.get_range();

            int index = positions_block.block_data[i] - range.first;

            ColumnTypeConstants::raw_timestamp value = data[index];
            tm *ltm = localtime(&value);

            GroupByYearMonthPosition key = GroupByYearMonthPosition(positions_block.block_data[i], 1900 + ltm->tm_year, 1 + ltm->tm_mon);
            qualified_positions_with_key_block.push_data(key, num_qualified_tuples);
            ++num_qualified_tuples;

            if (qualified_positions_with_key_block.is_full(num_qualified_tuples))
            {
                qualified_positions_with_key_block.write_data(this->position_key_output_file);
                num_qualified_tuples = 0;
                qualified_positions_with_key_block.clear();
            }
        }
    }

    std::cout << "Got qualified tuples" << std::endl;

    if (num_qualified_tuples > 0)
    {
        qualified_positions_with_key_block.write_data(this->position_key_output_file, num_qualified_tuples);
        num_qualified_tuples = 0;
    }

    this->position_input_file.close();
    this->position_key_output_file.close();
    this->data_file.close();
}

// For temp and humidity
void GroupBy::save_aggregation(std::string position_input_file_name, std::string max_file_name, std::string min_file_name, std::string data_file_name, std::map<std::string, float> &aggr_min, std::map<std::string, float> &aggr_max)
{
    this->position_input_file.open(position_input_file_name, std::ios::binary);
    this->data_file.open(data_file_name, std::ios::binary);

    int position_block_size = this->block_size;
    // std::vector<GroupByYearMonthPosition> positions(position_block_size / sizeof(GroupByYearMonthPosition));
    Block<GroupByYearMonthPosition> positions_block(position_block_size);

    Block<float> data_block = Block<float>(this->block_size);

    std::map<std::string, MaxAggregation<float>> max_aggr;
    std::map<std::string, MinAggregation<float>> min_aggr;

    while (this->position_input_file.good())
    {
        // this->position_input_file.read(reinterpret_cast<char *>(positions.data()), positions.size() * sizeof(GroupByYearMonthPosition));
        bool status = positions_block.read_next_block(this->position_input_file);

        for (int i = 0; i < positions_block.get_data().size(); ++i)
        {
            //std::cout << "Position[i]: " << positions_block.block_data[i].position << " " << positions_block.block_data[i].key << std::endl;
            data_block.read_data(this->data_file, positions_block.block_data[i].position, false);
            std::vector<float> data = data_block.get_data();
            if (data.size() == 0) break;
            std::pair<int, int> range = data_block.get_range();

            int index = positions_block.block_data[i].position - range.first;

            float value = data[index];

            if (value == -1)
                continue; // we encode -1 for missing values

            std::string key(positions_block.block_data[i].key);

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

    // std::cout << "Found max/min" << std::endl;

    this->position_input_file.close();
    this->position_key_output_file.close();
    this->data_file.close();

    std::map<std::string, Predicate<float> *> min_compare;
    std::map<std::string, Predicate<float> *> max_compare;

    for (auto it = max_aggr.begin(); it != max_aggr.end(); ++it)
    {
        aggr_max[it->first] = it->second.compute_value();
        // std::cout << it->first << " : " << it->second.compute_value() << "\n";
        max_compare[it->first] = new AtomicPredicate("=", it->second.compute_value());
    }

    std::cout << '\n';
    for (auto it = min_aggr.begin(); it != min_aggr.end(); ++it)
    {
        aggr_min[it->first] = it->second.compute_value();
        // std::cout << it->first << " : " << it->second.compute_value() << "\n";
        min_compare[it->first] = new AtomicPredicate("=", it->second.compute_value());
    }

    this->position_input_file.open(position_input_file_name, std::ios::binary);
    this->data_file.open(data_file_name, std::ios::binary);

    std::ofstream max_output_pos;
    std::ofstream min_output_pos;

    max_output_pos.open(max_file_name, std::ios::binary);
    min_output_pos.open(min_file_name, std::ios::binary);

    positions_block = Block<GroupByYearMonthPosition>(position_block_size);

    // std::vector<ColumnTypeConstants::position> qualified_positions_min;
    int num_qualified_tuples_min = 0;
    Block<ColumnTypeConstants::position> qualified_positions_min_block(position_block_size);

    // std::vector<ColumnTypeConstants::position> qualified_positions_max;
    int num_qualified_tuples_max = 0;
    Block<ColumnTypeConstants::position> qualified_positions_max_block(position_block_size);

    while (this->position_input_file.good())
    {
        // this->position_input_file.read(reinterpret_cast<char *>(positions.data()), positions.size() * sizeof(GroupByYearMonthPosition));
        bool status = positions_block.read_next_block(this->position_input_file);

        for (int i = 0; i < positions_block.get_data().size(); ++i)
        {
            data_block.read_data(this->data_file, positions_block.block_data[i].position, false);
            std::vector<float> data = data_block.get_data();
            if (data.size() == 0) break;
            std::pair<int, int> range = data_block.get_range();

            int index = positions_block.block_data[i].position - range.first;

            float value = data[index];

            if (value == -1)
                continue; // we encode -1 for missing values

            std::string key(positions_block.block_data[i].key);

            if (min_compare[key]->evaluate_expr(value))
            {
                // qualified_positions_min.push_back(positions_block.block_data[i].position);
                qualified_positions_min_block.push_data(positions_block.block_data[i].position, num_qualified_tuples_min);
                num_qualified_tuples_min++;
            }
            if (max_compare[key]->evaluate_expr(value))
            {
                // qualified_positions_max.push_back(positions_block.block_data[i].position);
                qualified_positions_max_block.push_data(positions_block.block_data[i].position, num_qualified_tuples_max);
                num_qualified_tuples_max++;
            }

            // Min
            if (qualified_positions_min_block.is_full(num_qualified_tuples_min))
            {
                qualified_positions_min_block.write_data(min_output_pos);
                num_qualified_tuples_min = 0;
                qualified_positions_min_block.clear();
            }

            // Max
            if (qualified_positions_max_block.is_full(num_qualified_tuples_max))
            {
                qualified_positions_max_block.write_data(max_output_pos);
                num_qualified_tuples_max = 0;
                qualified_positions_max_block.clear();
            }
        }
    }

    if (num_qualified_tuples_min > 0)
    {
        qualified_positions_min_block.write_data(min_output_pos, num_qualified_tuples_min);
        num_qualified_tuples_min = 0;
        qualified_positions_min_block.clear();
    }

    if (num_qualified_tuples_max > 0)
    {
        qualified_positions_max_block.write_data(max_output_pos, num_qualified_tuples_max);
        num_qualified_tuples_max = 0;
        qualified_positions_max_block.clear();
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
