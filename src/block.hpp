#ifndef BLOCK_H
#define BLOCK_H

#include <vector>
#include <iostream>
#include <fstream>

#include "constants.hpp"

template <typename T>
class Block
/**
 * Block class which is agnostic to the type of data it will store; it is simply storing a fixed amount of data.
 * The interface will accept a target position (row number in the table) where it must read data, then read from/write to the target block where the data is present.
 */
{
private:
    int block_size; // in bytes
    int start_position;
    int end_position;
    int num_elements;

public:
    /**
     * all method definitions which rely on the template T need to be defined in the header file itself
     * https://stackoverflow.com/a/14914168/17022186
     */
    std::vector<T> block_data;
    int curr_start_of_block;
    Block();
    Block(int block_size);
    void read_data(std::ifstream &fin, int target_pos, bool verbose);
    void print_value(T ele);
    void push_data(T ele, int index);
    void write_data(std::ofstream &fout);
    bool read_next_block(std::ifstream &fin);
    void print_data();
    bool is_full(int num_filled);
    void clear();

    std::vector<T> get_data(); // return all the data
    std::pair<int, int> get_range();
};

template <typename T>
inline Block<T>::Block()
{
    this->block_size = 0;
    this->curr_start_of_block = -1;
    this->start_position = -1;
    this->end_position = -1;
}

template <typename T>
inline Block<T>::Block(int block_size)
{
    this->block_size = block_size;
    this->curr_start_of_block = -1;
    this->num_elements = this->block_size / sizeof(T);
    this->block_data.resize(this->num_elements);
    this->start_position = -1;
    this->end_position = -1;
}

template <>
inline Block<bool>::Block(int block_size)
{
    this->block_size = block_size;
    this->num_elements = this->block_size * 8;
    this->block_data.resize(this->num_elements);
    this->start_position = -1;
    this->end_position = -1;
}

template <typename T>
inline void Block<T>::print_value(T ele)
{
    std::cout << ele << std::endl;
}

template <typename T>
inline std::vector<T> Block<T>::get_data()
{
    return this->block_data;
}

template <>
inline void Block<bool>::print_value(bool ele)
{
    if (!ele)
        std::cout << "Changi" << std::endl;
    else
        std::cout << "Paya Lebar" << std::endl;
}

template <>
inline void Block<__int8>::print_value(__int8 ele)
{
    std::cout << (int)ele << std::endl;
}

template <typename T>
inline void Block<T>::read_data(std::ifstream &fin, int target_pos, bool verbose)
{
    int bytes_offset = target_pos * sizeof(T);
    int block_offset = (bytes_offset % this->block_size) / sizeof(T);
    int start_of_block = (bytes_offset / this->block_size) * this->block_size; // so that we can seek to that number of bytes

    if (start_of_block == this->curr_start_of_block)
    {
        // std::cout << "Current block already contains target position.\n";
        return;
    }
    else
    {
        // std::cout << "Reading Block\n";
        this->curr_start_of_block = start_of_block;
    }

    fin.seekg(start_of_block);
    fin.read(reinterpret_cast<char *>(this->block_data.data()), this->block_data.size() * sizeof(T));

    if (verbose)
        this->print_value(this->block_data.at(block_offset));

    this->start_position = (start_of_block / this->block_size) * (this->block_size / sizeof(T));
    this->end_position = start_position + this->block_data.size();
    return;
}

template <>
inline void Block<bool>::read_data(std::ifstream &fin, int target_pos, bool verbose)
{
    int bytes_offset = target_pos / 8; // starting byte number
    int block_offset = target_pos % (this->block_size * 8);
    int start_of_block = (bytes_offset / this->block_size) * this->block_size;

    if (start_of_block == this->curr_start_of_block)
    {
        // std::cout << "Current block already contains target position.\n";
        return;
    }
    else
    {
        // std::cout << "Reading Block\n";
        this->curr_start_of_block = start_of_block;
    }

    int num_elements = this->block_size / sizeof(uint8_t);

    uint8_t nums[num_elements];

    fin.seekg(start_of_block);
    fin.read((char *)&nums, num_elements * sizeof(uint8_t));

    for (int idx = 0; idx < this->block_size; ++idx)
    {
        std::vector<bool> x(8, false);
        for (uint8_t mask = 1, i = 0; mask > 0 && i < 8; ++i, mask <<= 1)
            x.at(i) = nums[idx] & mask;

        for (int i = 0; i < x.size(); ++i)
            this->block_data[idx * 8 + i] = x.at(i);
    }

    this->start_position = (start_of_block / this->block_size) * (this->block_size * 8);
    this->end_position = start_position + this->block_data.size();

    if (verbose)
        this->print_value(this->block_data.at(block_offset));
    return;
}

template <typename T>
inline void Block<T>::write_data(std::ofstream &fout)
{
    fout.write((char *)&block_data[0], this->block_data.size() * sizeof(T));
}

template <typename T>
inline void Block<T>::push_data(T ele, int index)
{
    this->block_data.at(index) = ele;
    // this->print_data();
}

template <typename T>
inline void Block<T>::print_data()
{
    for (T ele : this->block_data)
        std::cout << ele << " ";
    std::cout << std::endl;
}

template <typename T>
inline bool Block<T>::read_next_block(std::ifstream &fin)
{
    this->clear();
    int next_start_index = (this->curr_start_of_block == -1) ? 0 : (this->curr_start_of_block / sizeof(T) + this->num_elements);
    // if (next_start_index % 10000 == 0)
    //     std::cout << "Next start index:" << next_start_index << std::endl;
    if (next_start_index > ProgramConstants::num_columns)
        return false;
    this->read_data(fin, next_start_index, false);
    return true;
}

template <typename T>
inline bool Block<T>::is_full(int num_filled)
{
    // std::cout << num_filled << this->num_elements << std::endl;
    return (num_filled == this->num_elements);
}

template <>
inline void Block<__int8>::print_data()
{
    for (__int8 ele : this->block_data)
        std::cout << (int)ele << " ";
    std::cout << std::endl;
}

template <typename T>
inline std::pair<int, int> Block<T>::get_range()
{
    return std::make_pair(this->start_position, this->end_position);
}

template <typename T>
inline void Block<T>::clear()
{
    this->block_data.clear();
    this->block_data.resize(this->num_elements);
}

#endif