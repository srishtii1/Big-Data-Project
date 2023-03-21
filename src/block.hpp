#ifndef BLOCK_H
#define BLOCK_H

#include <vector>
#include <iostream>
#include <fstream>

template <typename T>
class Block
/**
 * Block class which is agnostic to the type of data it will store; it is simply storing a fixed amount of data.
 * The interface will accept a target position (row number in the table) where it must read data, then read from/write to the target block where the data is present.
 */
{
private:
    std::vector<T> block_data;
    int block_size; // in bytes
public:
    /**
     * all method definitions which rely on the template T need to be defined in the header file itself
     * https://stackoverflow.com/a/14914168/17022186
     */
    Block(int block_size);
    void read_data(std::ifstream &fin, int target_pos);
    void print_value(T ele);
    void write_data(std::ofstream &fout, int start_pos);
    void print_data();
};

template <typename T>
Block<T>::Block(int block_size)
{
    this->block_size = block_size;
    this->block_data.resize(this->block_size / sizeof(T));
}

template <>
Block<bool>::Block(int block_size)
{
    this->block_size = block_size;
    this->block_data.resize(this->block_size * 8);
}

template <typename T>
void Block<T>::read_data(std::ifstream &fin, int target_pos)
{
    // std::cout << "Incoming target pos: " << target_pos << std::endl;
    // std::cout << "Data type size: " << sizeof(T) << std::endl;
    // std::cout << "Block size: " << this->block_size << std::endl;
    int bytes_offset = target_pos * sizeof(T);
    int block_offset = (bytes_offset % this->block_size) / sizeof(T);
    int start_of_block = (bytes_offset / this->block_size) * this->block_size; // so that we can seek to that number of bytes
    // std::cout << "Bytes offset: " << bytes_offset << std::endl;
    // std::cout << "Start of target block: " << start_of_block << std::endl;
    // std::cout << "Block offset: " << block_offset << std::endl;
    fin.seekg(start_of_block);
    fin.read(reinterpret_cast<char *>(this->block_data.data()), this->block_size);
    // std::cout << "Block length: " << this->block_data.size() << std::endl;
    this->print_value(this->block_data.at(block_offset));
    return;
}

template <>
void Block<bool>::read_data(std::ifstream &fin, int target_pos)
{
    // TODO: fix this function
    // std::cout << "Incoming target pos: " << target_pos << std::endl;
    // std::cout << "Block size: " << this->block_size << std::endl;
    int bytes_offset = target_pos / 8; // starting byte number
    int block_offset = target_pos % (this->block_size * 8);
    int start_of_block = (bytes_offset / this->block_size) * this->block_size;
    // std::cout << "Block size: " << this->block_size << std::endl;
    uint8_t nums[this->block_size];

    // std::cout << "Bytes offset: " << bytes_offset << std::endl;
    // std::cout << "Start of target block: " << start_of_block << std::endl;
    // std::cout << "Block offset: " << block_offset << std::endl;
    fin.seekg(start_of_block);
    fin.read((char *)&nums, this->block_size);

    for (int idx = 0; idx < this->block_size; ++idx)
    {
        std::vector<bool> x(8, false);
        for (uint8_t mask = 1, i = 0; mask > 0 && i < 8; ++i, mask <<= 1)
            x.at(i) = nums[idx] & mask;

        for (int i = 0; i < x.size(); ++i)
            this->block_data[idx * 8 + i] = x.at(i);
    }
    // std::cout << "Block length: " << this->block_data.size() << std::endl;
    this->print_value(this->block_data.at(block_offset));
    return;
}

template <typename T>
void Block<T>::write_data(std::ofstream &fout, int start_pos)
{
    return;
}

template <typename T>
void Block<T>::print_value(T ele)
{
    std::cout << ele << std::endl;
}

template <>
void Block<__int8>::print_value(__int8 ele)
{
    std::cout << (int)ele << std::endl;
}

template <typename T>
void Block<T>::print_data()
{
    for (T ele : this->block_data)
        std::cout << ele << std::endl;
}

template <>
void Block<__int8>::print_data()
{
    for (__int8 ele : this->block_data)
        std::cout << (int)ele << std::endl;
}

#endif