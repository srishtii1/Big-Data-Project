/**
 * @file binary_search_filter.hpp
 * @author Atul, Siddharth
 * @brief Header file for binary search based filter
 * @version 0.1
 * @date 2023-04-20
 *
 * @copyright Copyright (c) 2023
 *
 */
#if !defined(BINARY_SEARCH_FILTER_H)
#define BINARY_SEARCH_FILTER_H

#include <fstream>
#include "predicate.hpp"
#include "../../block.hpp"

/**
 * @brief Binary Search Filter class
 * @tparam T
 */
template <typename T>
class BinarySearchFilter
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
    BinarySearchFilter(std::string position_input_file_name, std::string position_output_file_name, std::string data_file_name, int block_size);
    ~BinarySearchFilter();
    void process_filter(std::vector<AtomicPredicate<T>> preds, int start, int end, bool verbose = false);
};

/**
 * @brief Construct a new Binary Search Filter< T>:: Binary Search Filter object
 * @tparam T
 * @param position_input_file_name: string file name of input qualified positions file
 * @param position_output_file_name: string file name of output qualified positions file
 * @param data_file_name: string file name of original column store data
 * @param block_size: desired block size
 */
template <typename T>
BinarySearchFilter<T>::BinarySearchFilter(std::string position_input_file_name, std::string position_output_file_name, std::string data_file_name, int block_size)
{
    this->position_input_file.open(position_input_file_name, std::ios::binary);
    this->position_output_file.open(position_output_file_name, std::ios::binary);
    this->data_file.open(data_file_name, std::ios::binary);
    this->block_size = block_size;
}

/**
 * @brief Method to process the filter using binary search
 * Note: Binary search will only work for >, <, <=, >= since these can be represented as increasing and decreasing functions
 * For = we need to compare with current value
 * @tparam T
 * @param preds: predicates to filter on; must be one of <, >, <=, >= as described above
 * @param start: starting position for bianry search
 * @param end: ending position for binary search
 * @param verbose: verbosity level
 */
template <typename T>
void BinarySearchFilter<T>::process_filter(std::vector<AtomicPredicate<T>> preds, int start, int end, bool verbose)
{
    int position_block_size = this->block_size;
    Block<ColumnTypeConstants::position> positions_block(position_block_size);

    Block<T> data_block = Block<T>(position_block_size);

    Block<ColumnTypeConstants::position> qualified_positions_block(position_block_size);
    int num_qualified_tuples = 0;

    int start_copy = start;
    int end_copy = end;

    int num_data_ios = 0;

    for (auto pred = preds.begin(); pred != preds.end(); ++pred)
    {
        int required_pos = -1;

        start = start_copy;
        end = end_copy;

        bool done = false;

        while (start <= end)
        {
            int mid = start + (end - start) / 2;

            bool read = data_block.read_data(this->data_file, mid, false);

            if (read)
                ++num_data_ios;

            std::vector<T> data = data_block.get_data();
            std::pair<int, int> range = data_block.get_range();

            if (pred->get_value() == data[0]) // possible first block
            {
                required_pos = range.first;
                end = range.first - 1;
            }

            else if (pred->get_value() > data[0] && pred->get_value() <= data[data.size() - 1])
            { // The first tuple exists in this block
                for (int i = 0; i < data.size(); ++i)
                {
                    if (pred->evaluate_expr(data[i]))
                    {
                        required_pos = range.first + i;
                        done = true;
                        break;
                    }
                }
            }

            else if (pred->get_value() > data[data.size() - 1])
            {
                start = range.second + 1;
            }

            else if (pred->get_value() < data[0])
            {
                end = range.first - 1;
            }

            if (done)
                break;
        }

        if (required_pos == -1)
            continue;

        while (true)
        {
            bool read = data_block.read_data(this->data_file, required_pos, false);

            if (read)
                ++num_data_ios;

            std::vector<T> data = data_block.get_data();
            std::pair<int, int> range = data_block.get_range();

            int index = required_pos - range.first;

            T value = data[index];

            if (pred->evaluate_expr(value))
            {
                qualified_positions_block.push_data(required_pos, num_qualified_tuples);
                ++num_qualified_tuples;

                if (qualified_positions_block.is_full(num_qualified_tuples))
                {
                    qualified_positions_block.write_data(this->position_output_file);
                    num_qualified_tuples = 0;
                    qualified_positions_block.clear();
                }
            }

            else
            {
                if (num_qualified_tuples > 0)
                {
                    qualified_positions_block.write_data(this->position_output_file, num_qualified_tuples);
                    num_qualified_tuples = 0;
                    qualified_positions_block.clear();
                }
                break;
            }

            ++required_pos;
        }
    }
    this->position_input_file.close();
    this->position_output_file.close();
    this->data_file.close();

    if (verbose)
        std::cout << "Number of Data IOs: " << num_data_ios << '\n';
}

/**
 * @brief Destroy the Binary Search Filter< T>:: Binary Search Filter object
 * Closes relevant files to avoid corruption
 * @tparam T
 */
template <typename T>
BinarySearchFilter<T>::~BinarySearchFilter()
{
    this->position_input_file.close();
    this->position_output_file.close();
    this->data_file.close();
}

#endif // BINARY_SEARCH_FILTER_H
