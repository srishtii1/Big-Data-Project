/**
 * @file filter.hpp
 * @author Atul, Siddharth, Srishti
 * @brief Header file for standard filter
 * @version 0.1
 * @date 2023-04-20
 *
 * @copyright Copyright (c) 2023
 *
 */
#if !defined(FILTER_H)
#define FILTER_H

#include <fstream>
#include "predicate.hpp"
#include "../../block.hpp"

/**
 * @brief Naive Filter class
 * @tparam T
 */
template <typename T>
class Filter
{
private:
    /**
     * @property position_input_file: input file stream of positions from previous step in query pipeline
     * @property position_output_file: output file stream where new qualified positions must be written
     * @property data_file: input file stream of original data column
     * @property block_size: desired block size
     */
    std::ifstream position_input_file;
    std::ofstream position_output_file;
    std::ifstream data_file;
    int block_size;

public:
    Filter(std::string position_input_file_name, std::string position_output_file_name, std::string data_file_name, int block_size);
    ~Filter();
    void process_filter(Predicate<T> &predicate, bool verbose = false); // Use the templates here
};

/**
 * @brief Construct a new Filter< T>:: Filter object
 * @tparam T
 * @param position_input_file_name: string file name of input qualified positions file
 * @param position_output_file_name: string file name of output qualified positions file
 * @param data_file_name: string file name of original column store data
 * @param block_size: desired block size
 */
template <typename T>
Filter<T>::Filter(std::string position_input_file_name, std::string position_output_file_name, std::string data_file_name, int block_size)
{
    this->position_input_file.open(position_input_file_name, std::ios::binary);
    this->position_output_file.open(position_output_file_name, std::ios::binary);
    this->data_file.open(data_file_name, std::ios::binary);
    this->block_size = block_size;
}

/**
 * @brief Method to process the filter using standard iterative filtering
 * @tparam T
 * @param pred: predicate to filter by
 * @param verbose: verbosity level
 */
template <typename T>
void Filter<T>::process_filter(Predicate<T> &pred, bool verbose)
{
    int position_block_size = this->block_size;
    Block<ColumnTypeConstants::position> positions_block(position_block_size);

    Block<T> data_block = Block<T>(position_block_size);

    Block<ColumnTypeConstants::position> qualified_positions_block(position_block_size);
    int num_qualified_tuples = 0;

    int num_data_ios = 0;

    while (this->position_input_file.good())
    {
        bool status = positions_block.read_next_block(this->position_input_file);

        for (int i = 0; i < positions_block.get_data().size(); ++i)
        {
            bool read = data_block.read_data(this->data_file, positions_block.block_data[i], false);

            if (read)
                ++num_data_ios;

            std::vector<T> data = data_block.get_data();
            if (data.size() == 0)
                break;
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

    if (verbose)
        std::cout << "Number of Data IOs: " << num_data_ios << '\n';
}

/**
 * @brief Destroy the Filter< T>:: Filter object
 * Closes relevant files to avoid corruption
 * @tparam T
 */
template <typename T>
Filter<T>::~Filter()
{
    this->position_input_file.close();
    this->position_output_file.close();
    this->data_file.close();
}

#endif // FILTER_H
