#ifndef BLOCK_H
#define BLOCK_H

#include <vector>
#include <iostream>
#include <fstream>

template <typename T>
class Block
/**
 * Block class which is agnostic to the type of data it will store; it is simply storing a fixed amount of data.
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
    void read_data(std::ifstream &fin, int start_pos);
    void write_data(std::ofstream &fout, int start_pos);
    void print_data();
};

template <typename T>
Block<T>::Block(int block_size)
{
    this->block_size = block_size;
    this->block_data.resize(this->block_size / sizeof(T));
}

template <typename T>
void Block<T>::read_data(std::ifstream &fin, int start_pos)
{
    std::cout << "About to read" << std::endl;
    start_pos = start_pos * sizeof(T); // so that we can seek to that number of bytes
    fin.seekg(start_pos);
    fin.read(reinterpret_cast<char *>(this->block_data.data()), this->block_size);
    std::cout << "Done reading" << std::endl;
    std::cout << this->block_data.size() << std::endl;
    return;
}

template <>
void Block<bool>::read_data(std::ifstream &fin, int start_pos)
{
    std::cout << "About to read" << std::endl;
    int rem_pos = start_pos % 8;
    int end_pos = 8 - rem_pos;
    start_pos = start_pos / 8;
    std::cout << "Block size " << this->block_size << std::endl;
    uint8_t nums[this->block_size];

    fin.seekg(start_pos);
    fin.read((char *)&nums, this->block_size);

    for (int idx = 0; idx < this->block_size; ++idx)
    {
        int start_index = idx == 0 ? rem_pos : 0;
        int end_index = idx == (this->block_size - 1) ? end_pos : 8;
        int start_mask = 1 << start_index;
        int array_size = end_index - start_index;
        std::vector<bool> x(array_size, false);
        for (uint8_t mask = start_mask, i = start_index; mask > 0 && i < end_index; ++i, mask <<= 1)
        {
            x.at(i - start_index) = nums[idx] & mask;
        }

        for (int i = 0; i < x.size(); ++i)
        {
            this->block_data.push_back(x[i]);
        }
    }

    std::cout << "Done reading" << std::endl;
    std::cout << this->block_data.size() << std::endl;
    return;
}

template <typename T>
void Block<T>::write_data(std::ofstream &fout, int start_pos)
{
    return;
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