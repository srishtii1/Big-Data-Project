#if !defined(FILTER_H)
#define FILTER_H

#include <fstream>
#include "predicate.hpp"
#include "../../block.hpp"

template <typename T>
class Filter
{
private:
    // position file name // if position file is nullptr then scan entire column // Read position into an object of Position / GroupBy Position
    // column file name
    // List of predicates
    // Save position file name along with groupby attribute? // Save into Position or GroupBy position
    
    // Instantiate Blocks. Move to the position and compare predicate. If yes save position in block.add_data
    // 1 block to read positions, 1 block to read column data, 1 block to write position
    std::ifstream position_input_file;
    std::ofstream position_output_file;
    std::ifstream data_file;

public:
    Filter(std::string position_input_file_name, std::string position_output_file_name, std::string data_file_name);
    ~Filter();
    void process_filter(Predicate<T>& predicate); // Use the templates here
};

template <typename T>
Filter<T>::Filter(std::string position_input_file_name, std::string position_output_file_name, std::string data_file_name)
{
    this->position_input_file.open(position_input_file_name, std::ios::binary);
    this->position_output_file.open(position_output_file_name, std::ios::binary);
    this->data_file.open(data_file_name, std::ios::binary);
}

template <typename T>
void Filter<T>::process_filter(Predicate<T>& pred)
{
    int position_block_size = 2048;
    std::vector<uint32_t> positions(position_block_size / sizeof(uint32_t));
    Block<T> data_block = Block<T>(2048);

    std::vector<uint32_t> qualififed_positions;
    int num_qualified_tuples = 0;

    while(this->position_input_file.good())
    {
        this->position_input_file.read(reinterpret_cast<char *>(positions.data()), positions.size() * sizeof(uint32_t));

        for (int i=0; i<positions.size(); ++i)
        {
            data_block.read_data(this->data_file, positions[i], false);
            std::vector<T> data = data_block.get_data();
            std::pair<int, int> range = data_block.get_range();

            int index = positions[i] - range.first;

            T value = data[index];

            if (pred.evaluate_expr(value))
            {
               ++num_qualified_tuples;
               qualififed_positions.push_back(positions[i]);
               if(num_qualified_tuples >= position_block_size / sizeof(uint32_t))
               {
                    this->position_output_file.write(reinterpret_cast<char *>(qualififed_positions.data()), qualififed_positions.size() * sizeof(uint32_t));
                    num_qualified_tuples = 0;
                    qualififed_positions.clear();
               }
            }
        }
    }

    if(num_qualified_tuples > 0)
    {
        this->position_output_file.write(reinterpret_cast<char *>(qualififed_positions.data()), qualififed_positions.size() * sizeof(uint32_t));
        num_qualified_tuples = 0;
        qualififed_positions.clear();
    }
}


template<typename T>
Filter<T>::~Filter()
{
    this->position_input_file.close();
    this->position_output_file.close();
    this->data_file.close();
}




#endif // FILTER_H
