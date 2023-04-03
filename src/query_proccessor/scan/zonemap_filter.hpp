#if !defined(ZONEMAPFILTER_H)
#define ZONEMAPFILTER_H

#include <fstream>
#include "predicate.hpp"
#include "../../block.hpp"
#include "../../zonemap/Zone.hpp"
#include <vector>

template <typename T>
class ZonemapFilter
{
private:
    std::ifstream position_input_file;
    std::ofstream position_output_file;
    std::ifstream data_file;
    std::ifstream zonemap_file;
    int block_size;
    std::vector<Zone<T>> zones;

public:
    ZonemapFilter(std::string position_input_file_name, std::string position_output_file_name, std::string data_file_name,std::string zonemap_file, int block_size);
    ~ZonemapFilter();
    void process_filter(std::vector<std::pair<AtomicPredicate<T>, AtomicPredicate<T>>> preds, bool verbose=false); // Use the templates here
};

template <typename T>
ZonemapFilter<T>::ZonemapFilter(std::string position_input_file_name, std::string position_output_file_name, std::string data_file_name, std::string zonemap_file, int block_size)
{
    this->position_input_file.open(position_input_file_name, std::ios::binary);
    this->position_output_file.open(position_output_file_name, std::ios::binary);
    this->data_file.open(data_file_name, std::ios::binary);
    this->zonemap_file.open(zonemap_file, std::ios::binary);
    this->block_size = block_size;

    //load zonemap
    Block<Zone<T>> zone_block(block_size);
    while (this->zonemap_file.good())
    {
        bool status = zone_block.read_next_block(this->zonemap_file);
        if (!status)
        {
            //std::cout<<"Error reading zonemap file"<<std::endl;
            break;
        }
        for (int i = 0; i < zone_block.get_data().size(); i++)
        {
            this->zones.push_back(zone_block.block_data[i]);
        }
    }

    std::cout << "Number of zones: " << this->zones.size() << '\n';
}

template <typename T>
void ZonemapFilter<T>::process_filter(std::vector<std::pair<AtomicPredicate<T>, AtomicPredicate<T>>> preds, bool verbose)
{
    int position_block_size = this->block_size;
    Block<ColumnTypeConstants::position> positions_block(position_block_size);
    std::vector<ColumnTypeConstants::position> positions(position_block_size / ColumnSizeConstants::position);

    Block<T> data_block = Block<T>(position_block_size);

    Block<ColumnTypeConstants::position> qualified_positions_block(position_block_size);
    std::vector<ColumnTypeConstants::position> qualified_positions;
    std::vector<int> qualified_block_indices;
    int num_qualified_tuples = 0;

    int num_data_ios = 0;
    
    for(int block_index = 0; block_index<zones.size(); ++block_index)
    {
        T ZoneMin = this->zones[block_index].getMin();
        T ZoneMax = this->zones[block_index].getMax();

        for (int i = 0; i < preds.size(); ++i)
        {
            auto pred_pair = preds[i];
            if ((pred_pair.first.evaluate_expr(ZoneMin) && pred_pair.second.evaluate_expr(ZoneMax)))
            {
                // std::cout << "ZoneMap: " << block_index << " = "<< ZoneMin << ", " << ZoneMax << '\n';
                qualified_block_indices.push_back(block_index);
                break;
            }
        }
    }
    
    int block_index = 0;
    int req_block_start_position;
    while (block_index < qualified_block_indices.size())
    {
        req_block_start_position = qualified_block_indices[block_index] * data_block.num_elements;

        bool read = data_block.read_data(this->data_file, req_block_start_position, false);
        
        if (read) ++num_data_ios;
        
        std::vector<T> data = data_block.get_data();
        if (data.size() == 0) break;
        
        std::pair<int, int> range = data_block.get_range();

        int data_index = req_block_start_position - range.first;
        while(data_index < data_block.num_elements)
        {
            T value = data[data_index];
            for (int i=0; i<preds.size(); ++i)
            {
                auto pred_pair = preds[i];
                if ((pred_pair.first.evaluate_expr(value) && pred_pair.second.evaluate_expr(value)))
                {
                    qualified_positions_block.push_data(req_block_start_position + data_index, num_qualified_tuples);
                    ++num_qualified_tuples; 
                    if (qualified_positions_block.is_full(num_qualified_tuples))
                    {
                        qualified_positions_block.write_data(this->position_output_file);
                        num_qualified_tuples = 0;
                        qualified_positions_block.clear();
                    }
                }
            }
            data_index++;
        }
        block_index++;
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

    if (verbose) std::cout << "Number of Data IOs: " << num_data_ios << '\n'; 
}

template <typename T>
ZonemapFilter<T>::~ZonemapFilter()
{
    this->position_input_file.close();
    this->position_output_file.close();
    this->data_file.close();
}

#endif // ZONEMAPFILTER_H
