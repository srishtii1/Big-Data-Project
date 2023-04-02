#if !defined(FILTER_H)
#define FILTER_H

#include <fstream>
#include "predicate.hpp"
#include "../../block.hpp"

template <typename T>
class Filter
{
private:
    std::ifstream position_input_file;
    std::ofstream position_output_file;
    std::ifstream data_file;
    int block_size;

public:
    Filter(std::string position_input_file_name, std::string position_output_file_name, std::string data_file_name, int block_size);
    ~Filter();
    void process_filter(Predicate<T> &predicate); // Use the templates here
};

template <typename T>
Filter<T>::Filter(std::string position_input_file_name, std::string position_output_file_name, std::string data_file_name, int block_size)
{
    this->position_input_file.open(position_input_file_name, std::ios::binary);
    this->position_output_file.open(position_output_file_name, std::ios::binary);
    this->data_file.open(data_file_name, std::ios::binary);
    this->block_size = block_size;
}

template <typename T>
void Filter<T>::process_filter(Predicate<T> &pred)
{
    int position_block_size = this->block_size;
    Block<ColumnTypeConstants::position> positions_block(position_block_size);
    std::vector<ColumnTypeConstants::position> positions(position_block_size / ColumnSizeConstants::position);

    Block<T> data_block = Block<T>(position_block_size);

    Block<ColumnTypeConstants::position> qualified_positions_block(position_block_size);
    std::vector<ColumnTypeConstants::position> qualified_positions;
    int num_qualified_tuples = 0;

    while (this->position_input_file.good())
    {
        bool status = positions_block.read_next_block(this->position_input_file);

        for (int i = 0; i < positions_block.get_data().size(); ++i)
        {
            data_block.read_data(this->data_file, positions_block.block_data[i], false);
            std::vector<T> data = data_block.get_data();
            if (data.size() == 0) break;
            std::pair<int, int> range = data_block.get_range();

            int index = positions_block.block_data[i] - range.first;

            T value = data[index];

            if (pred.evaluate_expr(value))
            {
                qualified_positions_block.push_data(positions_block.block_data[i], num_qualified_tuples);
                ++num_qualified_tuples;

                if (qualified_positions_block.is_full(num_qualified_tuples))
                {
                    qualified_positions_block.write_data(this->position_output_file, num_qualified_tuples);
                    num_qualified_tuples = 0;
                    qualified_positions_block.clear();
                }
            }
        }
    }

    if (num_qualified_tuples > 0)
    {
        qualified_positions_block.write_data(this->position_output_file, num_qualified_tuples);
        num_qualified_tuples = 0;
        qualified_positions_block.clear();
    }

    this->position_input_file.close();
    this->position_output_file.close();
    this->data_file.close();
}

template <typename T>
Filter<T>::~Filter()
{
    this->position_input_file.close();
    this->position_output_file.close();
    this->data_file.close();
}

#endif // FILTER_H
