#include "query_processing.hpp"
#include "query_proccessor/scan/predicate.hpp"
#include "query_proccessor/scan/filter.hpp"
#include "query_proccessor/scan/binary_search_filter.hpp"
#include "query_proccessor/groupby/groupby.hpp"

#include "query_proccessor/projection.hpp"

#include <map>

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
    year_position_block.write_data(fout);
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
    for (int i = 0; i < ProgramConstants::num_rows; i += num_years_in_block)
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

void QueryProcessor::process_query(std::string matric_num, uint16_t year1, uint16_t year2, bool city)
{
    
    // std::ifstream ifile;
    // ifile.open("data/column_store/temp/positions.dat", std::ios::binary);
    // while(ifile.good())
    // {
    //     uint32_t idx;
    //     ifile.read((char*)&idx, sizeof(idx));
    //     std::cout << idx << '\n';
    // }
    // ifile.close();

    // AtomicPredicate<ColumnTypeConstants::year> *p1 = new AtomicPredicate<ColumnTypeConstants::year>("=", year1);
    // AtomicPredicate<ColumnTypeConstants::year> *p2 = new AtomicPredicate<ColumnTypeConstants::year>("=", year2);
    // OrPredicate<ColumnTypeConstants::year> orPred = OrPredicate<ColumnTypeConstants::year>({p1, p2});

    AtomicPredicate<ColumnTypeConstants::year> p1 = AtomicPredicate<ColumnTypeConstants::year>("=", year1);
    AtomicPredicate<ColumnTypeConstants::year> p2 = AtomicPredicate<ColumnTypeConstants::year>("=", year2);

    // Filter Year
    // Filter<ColumnTypeConstants::year> year_filter = Filter<ColumnTypeConstants::year>("data/column_store/temp/positions.dat", "data/column_store/temp/temp1.dat", "data/column_store/year_encoded.dat", this->block_size);
    // // year_filter.process_filter(orPred);

    // std::ifstream ifile;
    // ifile.open("data/column_store/temp/temp1.dat", std::ios::binary);
    // while(ifile.good())
    // {
    //     uint32_t idx;
    //     ifile.read((char*)&idx, sizeof(idx));
    //     std::cout << idx << '\n';
    // }
    // ifile.close();

    // return;
    BinarySearchFilter<ColumnTypeConstants::year> year_filer_binary_search = BinarySearchFilter<ColumnTypeConstants::year>("data/column_store/temp/positions.dat", "data/column_store/temp/temp1.dat", "data/column_store/year_encoded.dat", this->block_size);
    year_filer_binary_search.process_filter({p1, p2}, 0, ProgramConstants::num_rows);

    // Filter City
    AtomicPredicate<ColumnTypeConstants::city> p3 = AtomicPredicate<ColumnTypeConstants::city>("=", city);
    // Filter<ColumnTypeConstants::city> city_filter = Filter<ColumnTypeConstants::city>("data/column_store/temp/temp1.dat", "data/column_store/temp/temp2.dat", "data/column_store/city_encoded.dat", this->block_size);
    // city_filter.process_filter(p3);

    // std::ifstream ifile;
    // ifile.open("data/column_store/temp/temp2.dat", std::ios::binary);
    // while(ifile.good())
    // {
    //     uint32_t idx;
    //     ifile.read((char*)&idx, sizeof(idx));
    //     std::cout << idx << '\n';
    // }
    // ifile.close();

    // return;

    // Save Group By Key for the required positions along with posiiton

    GroupBy groupby_year_month = GroupBy(this->block_size);
    groupby_year_month.save_groupby_key("data/column_store/temp/temp2.dat", "data/column_store/temp/temp3.dat", "data/column_store/raw_timestamp_encoded.dat");

    // std::ifstream ifile;
    // ifile.open("data/column_store/temp/temp3.dat", std::ios::binary);
    // while(ifile.good())
    // {
    //     GroupByYearMonthPosition idx;
    //     ifile.read((char*)&idx, sizeof(idx));
    //     std::cout << idx.key << " : " << idx.position << '\n';
    // }
    // ifile.close();

    // return;

    std::map<std::string, ColumnTypeConstants::temperature> min_temp;
    std::map<std::string, ColumnTypeConstants::temperature> max_temp;
    groupby_year_month.save_aggregation("data/column_store/temp/temp3.dat", "data/column_store/temp/max_temp.dat", "data/column_store/temp/min_temp.dat", "data/column_store/temperature_encoded.dat", min_temp, max_temp);

    // std::ifstream ifile;
    // ifile.open("data/column_store/temp/max_temp.dat", std::ios::binary);
    // while(ifile.good())
    // {
    //     int idx;
    //     ifile.read((char*)&idx, sizeof(idx));
    //     std::cout << idx << '\n';
    // }
    // ifile.close();
    // return;


    std::cout << "\nMin Temp\n";
    for (auto it = min_temp.begin(); it != min_temp.end(); ++it)
    {
        std::cout << it->first << " : " << it->second << '\n';
    }

    std::cout << "\nMax Temp\n";
    for (auto it = max_temp.begin(); it != max_temp.end(); ++it)
    {
        std::cout << it->first << " : " << it->second << '\n';
    }

    std::map<std::string, ColumnTypeConstants::humidity> min_humid;
    std::map<std::string, ColumnTypeConstants::humidity> max_humid;
    groupby_year_month.save_aggregation("data/column_store/temp/temp3.dat", "data/column_store/temp/max_humid.dat", "data/column_store/temp/min_humid.dat", "data/column_store/humidity_encoded.dat", min_humid, max_humid);

    std::cout << "\nMin Humid\n";
    for (auto it = min_humid.begin(); it != min_humid.end(); ++it)
    {
        std::cout << it->first << " : " << it->second << '\n';
    }

    std::cout << "\nMax Humid\n";
    for (auto it = max_humid.begin(); it != max_humid.end(); ++it)
    {
        std::cout << it->first << " : " << it->second << '\n';
    }

    std::string output_file_name = "data/output/ScanResult_" + matric_num + ".csv";

    std::ofstream output_file;
    output_file.open(output_file_name);
    output_file << "Date,Station,Category,Value\n";

    std::string position_file_name = "";
    std::string proj_file = "data/column_store/raw_timestamp_encoded.dat";

    Projection proj;

    position_file_name = "data/column_store/temp/max_temp.dat";
    proj.save_result(position_file_name, output_file, proj_file, city == 0 ? "Changi" : "Paya Lebar", "Max Temperature", max_temp);

    position_file_name = "data/column_store/temp/min_temp.dat";
    proj.save_result(position_file_name, output_file, proj_file, city == 0 ? "Changi" : "Paya Lebar", "Min Temperature", min_temp);

    position_file_name = "data/column_store/temp/max_humid.dat";
    proj.save_result(position_file_name, output_file, proj_file, city == 0 ? "Changi" : "Paya Lebar", "Max Humidity", max_humid);

    position_file_name = "data/column_store/temp/min_humid.dat";
    proj.save_result(position_file_name, output_file, proj_file, city == 0 ? "Changi" : "Paya Lebar", "Min Humidity", min_temp);

    output_file.close();
}