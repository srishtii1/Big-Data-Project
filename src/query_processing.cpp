#include "query_processing.hpp"
#include "query_proccessor/scan/predicate.hpp"

QueryProcessor::QueryProcessor(int block_size)
{
    this->block_size = block_size;
    year_block = Block<ColumnTypeConstants::year>(block_size);
    month_block = Block<ColumnTypeConstants::month>(block_size);
    day_block = Block<ColumnTypeConstants::day>(block_size);

    time_block = Block<ColumnTypeConstants::time>(block_size);

    raw_timestamp_block = Block<ColumnTypeConstants::raw_timestamp>(block_size);

    city_block = Block<ColumnTypeConstants::city>(block_size);

    temperature_block = Block<ColumnTypeConstants::temperature>(block_size);
    humidity_block = Block<ColumnTypeConstants::humidity>(block_size);
}

void QueryProcessor::output_to_year_pos_file(Block<ColumnTypeConstants::position> &year_position_block, int &write_pos, int &pos_index)
{
    // std::cout << "Outputting to file!" << std::endl;
    std::ofstream fout("tmp/year_pos.dat", std::ios_base::app | std::ios::binary);
    year_position_block.write_data(fout, write_pos);
    // for (int pos : year_position_block.block_data)
    //     std::cout << pos << " ";
    // std::cout << std::endl;
    write_pos += year_position_block.block_data.size();

    fout.close();
    pos_index = 0;
    std::fill(year_position_block.block_data.begin(), year_position_block.block_data.end(), -1);
}

void QueryProcessor::get_year_pos(int year1, int year2)
{
    std::ifstream fin;
    fin.open("data/column_store/year_encoded.dat");
    std::ofstream fout("tmp/year_pos.dat", std::ios::out | std::ios::binary);
    fout.close();
    int num_years_in_block = this->block_size / ColumnSizeConstants::year;
    Block<ColumnTypeConstants::position> year_position_block(this->block_size);
    int pos_index = 0;
    int write_pos = 0;
    for (int i = 0; i < ProgramConstants::num_columns; i += num_years_in_block)
    {
        this->year_block.read_data(fin, i, false);
        for (auto it = this->year_block.block_data.begin(); it != this->year_block.block_data.end(); it++)
        {
            if ((*it == year1) || (*it == year2))
            {
                int index = i + std::distance(this->year_block.block_data.begin(), it);
                year_position_block.block_data.at(pos_index) = index;

                pos_index++;
                if (pos_index == (year_position_block.block_data.size()))
                {
                    this->output_to_year_pos_file(year_position_block, write_pos, pos_index);
                }
            }
        }
    }
    if (pos_index != 0)
    {
        this->output_to_year_pos_file(year_position_block, write_pos, pos_index);
    }

    fin.close();
    return;
}

std::vector<int> QueryProcessor::get_month_pos(int month, std::string year_pos_file)
{
    std::ifstream fin;
    fin.open("data/column_store/month_encoded.dat");
    int num_months_in_block = this->block_size / ColumnSizeConstants::month;
    std::vector<int> month_positions;
    // for (auto year_pos = year_position.begin(); year_pos != year_position.end(); ++year_pos)
    // {
    //     this->month_block.read_data(fin, *year_pos);
    //     for (auto it = this->month_block.block_data.begin(); it != this->month_block.block_data.end(); it++)
    //     {
    //         if (*it == month)
    //         {
    //             int index = std::distance(this->month_block.block_data.begin(), it);
    //             month_positions.push_back(index);
    //         }
    //     }
    // }
    fin.close();
    return month_positions;
}

void QueryProcessor::print_year_pos()
{
    std::ifstream fin;
    fin.open("tmp/year_pos.dat", std::ios::binary);
    // fin.seekg(0, std::ios::end);
    // int file_size = fin.tellg();
    // int num_blocks = file_size / ColumnSizeConstants::position;

    Block<ColumnTypeConstants::position> year_position_block(this->block_size);
    int read_pos = 0;
    while (fin.good())
    {
        year_position_block.read_data(fin, read_pos, false);
        year_position_block.print_data();
        // bool zeros = std::all_of(year_position_block.block_data.begin(), year_position_block.block_data.end(), [](int i)
        //                          { return i == 0; });
        // if (zeros)
        //     return;
        read_pos += year_position_block.block_data.size();
    }
}

void QueryProcessor::process_query(int year1, int year2, bool city)
{
    // this->get_year_pos(year1, year2);
    // std::cout << "Year 1 " << year1 << " Year 2 " << year2 << std ::endl;
    // this->print_year_pos();

    // Filter Year using binary search on zonemap since its sorted , create 2 predicates = year1, = year2
    
    Predicate<int> p1 = Predicate<int>("=", year1);
    Predicate<int> p2 = Predicate<int>("=", year2);
    OrPredicate<int> orPred = OrPredicate<int>({p1, p2});
    // Create Filter object and scan results

    // Filter City (Create Predicate and Filter)
    Predicate<bool> p3 = Predicate<bool>("=", city);

    // Save Group By Key for the required positions

    // Aggregate Fn over group by key
    // For each group by key create a new aggregate fn
    // Scan Temp
    // Scan Hummidity

    // Iterate through again to compare = aggr value

    // Load positions and get YYYY-MM-DD for each category

}