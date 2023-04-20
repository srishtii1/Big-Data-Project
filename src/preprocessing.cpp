#include "preprocessing.hpp"
#include "block.hpp"
#include "zonemap/Zone.hpp"
#include "vector"

/**
 * @brief Preprocessing function to write binary values in chunks to a file
 * This function chunks up groups of 8 bits into a single byte to output to a file, to store in a compressed manner
 * @param fout: file output stream to be written to
 * @param x: vector of boolean values to be written
 */

// Group into 8s if x.size() % 8 != 0
void binary_write(std::ofstream &fout, std::vector<bool> &x)
{
    while (x.size() < 8)
        x.push_back(0); // Make it size of 1 byte

    std::vector<bool>::size_type n = x.size();

    uint8_t code = 0;
    for (uint8_t mask = 1, i = 0; mask > 0 && i < n; ++i, mask <<= 1)
        if (x.at(i))
            code |= mask;
    fout.write((const char *)&code, sizeof(uint8_t));
}

/**
 * @brief Debug function to read the binary values written as described above
 *
 * @param fin: file input stream to read from
 * @param num_bytes: number of bytes to be read from input file
 * @return std::vector<bool>
 */
std::vector<bool> binary_read(std::ifstream &fin, int num_bytes)
{

    std::vector<bool> values;

    uint8_t nums[num_bytes];

    fin.read((char *)&nums, num_bytes * sizeof(nums[0]));

    for (int idx = 0; idx < num_bytes; ++idx)
    {
        std::vector<bool> x(8, false);
        for (uint8_t mask = 1, i = 0; mask > 0 && i < 8; ++i, mask <<= 1)
            x.at(i) = nums[idx] & mask;

        for (int i = 0; i < x.size(); ++i)
        {
            values.push_back(x[i]);
        }
    }

    return values;
}

/**
 * @brief Function to check existence of column store
 *
 * @return bool
 */
bool check_if_col_store_exists()
{
    // will fail if two programs access file simultanouesly; ideally shouldn't occur in this case
    bool exists = true;
    for (auto it = FileNameConstants::file_names.begin(); it != FileNameConstants::file_names.end(); ++it)
    {
        std::ifstream input_file("data/column_store/" + it->second, std::ios::binary);
        exists = exists && input_file.good();
    }
    return exists;
}

/**
 * @brief Function to preprocess the input database CSV and output to column store format
 */
void preprocess_csv()
{
    bool exists = check_if_col_store_exists();

    std::ifstream input_file;

    if (!exists)
    {

        input_file.open("data/SingaporeWeather.csv", std::ios::in);

        std::unordered_map<std::string, std::ofstream *> file_map;

        std::vector<bool> city_compact;
        int vec_size = 0;

        for (auto it = FileNameConstants::file_names.begin(); it != FileNameConstants::file_names.end(); ++it)
        {
            std::ofstream *output_file = new std::ofstream("data/column_store/" + it->second, std::ios::out | std::ios::binary);
            file_map[it->first] = output_file;
        }

        std::vector<std::string> row;

        std::string line, word;

        uint32_t counter = 0;

        uint32_t idx = 0;

        while (input_file.good())
        {
            row.clear();
            getline(input_file, line);
            std::stringstream s(line);

            ++counter;

            while (getline(s, word, ','))
            {
                row.push_back(word);
            }

            if (counter <= 1)
                continue; // Skip header

            file_map["positions"]->write((char *)&idx, (ColumnSizeConstants::position));
            ++idx;

            if (row[3] == "M")
                row[3] = "-1000.00";
            if (row[4] == "M")
                row[4] = "-1000.00";

            // Process Timestamp
            auto timestamp = row[1];

            // YYYY-MM-DD HH:MM
            ColumnTypeConstants::year year = (ColumnTypeConstants::year)(std::stoi(timestamp.substr(0, 4)));
            ColumnTypeConstants::month month = (ColumnTypeConstants::month)std::stoi(timestamp.substr(5, 2));
            ColumnTypeConstants::day day = (ColumnTypeConstants::day)std::stoi(timestamp.substr(8, 2));

            __int8 hour = (__int8)std::stoi(timestamp.substr(11, 2));
            __int8 minute = (__int8)std::stoi(timestamp.substr(14, 2));

            file_map["year"]->write((char *)&year, (ColumnSizeConstants::year));
            file_map["month"]->write((char *)&month, (ColumnSizeConstants::month));
            file_map["day"]->write((char *)&day, (ColumnSizeConstants::day));

            // encode the time of the day as one unsigned short instead of 2; since time is measured at the granularity of every 30 minutes
            ColumnTypeConstants::time time = (hour * 60) + minute;

            std::tm time_struct = {};
            std::istringstream ss(timestamp);

            if (ss >> std::get_time(&time_struct, "%Y-%m-%d %H:%M"))
            {
                // std::cout << std::put_time(&time_struct, "%c") << "\n"
                // << std::mktime(&time_struct) << "\n";
                time_t raw_timestamp = std::mktime(&time_struct);
                file_map["raw_timestamp"]->write((char *)&raw_timestamp, (ColumnSizeConstants::raw_timestamp));
            }
            else
            {
                std::cout << "Parse failed\n";
            }

            file_map["time"]->write((char *)&time, (ColumnSizeConstants::time));

            // Process Station
            auto station = row[2];
            bool station_encoded = station == "Changi" ? false : true;

            city_compact.push_back(station_encoded);
            ++vec_size;

            if (vec_size == 8)
            {
                binary_write(*file_map["city"], city_compact);
                city_compact.clear();
                vec_size = 0;
            }

            // Process Temperature
            float temperature = std::stof(row[3]);
            file_map["temperature"]->write((char *)&temperature, (ColumnSizeConstants::temperature));

            // Process Humidity
            float humidity = std::stof(row[4]);
            file_map["humidity"]->write((char *)&humidity, (ColumnSizeConstants::humidity));
        }

        // final write
        if (vec_size != 0)
        {
            binary_write(*file_map["city"], city_compact);
            city_compact.clear();
            vec_size = 0;
        }

        for (auto it = file_map.begin(); it != file_map.end(); ++it)
        {
            it->second->close();
        }

        input_file.close();
    }
    else
    {
        std::cout << "Using existing column store!" << std::endl;
    }

    std::cout << "\n";
    input_file.close();
}

/**
 * @brief Function to create zone map for year column
 * This function creates a zone map for the year column and saves the zone map to disk
 * @param block_size: Desired block size
 */

void createZonemap(int block_size)
{
    std::ifstream yearDataStream("data/column_store/" + FileNameConstants::year, std::ios::binary);
    int num_records_per_block = block_size / ColumnSizeConstants::year;
    std::cout << "Num Records per block: " << num_records_per_block << '\n';
    Block<ColumnTypeConstants::year> year_read_block = Block<ColumnTypeConstants::year>(block_size);
    std::ofstream zoneOutStream("data/zone_maps/year_zones.dat", std::ios::out | std::ios::binary);
    Block<Zone<ColumnTypeConstants::year>> write_block = Block<Zone<ColumnTypeConstants::year>>(block_size);

    int block_index = 0;
    int blk_ptr = 0;

    for (int pos = 0; pos < ProgramConstants::num_rows; pos += num_records_per_block)
    {
        year_read_block.read_data(yearDataStream, pos, false);
        // Since the dates are sorted, we can just check the first and last element of the block
        // to determine the range of the block
        ColumnTypeConstants::year year1 = year_read_block.block_data.front();
        ColumnTypeConstants::year year2 = year_read_block.block_data.back();
        Zone<ColumnTypeConstants::year> zone = Zone(block_index, year1, year2);
        write_block.push_data(zone, blk_ptr);
        blk_ptr++;
        if (write_block.is_full(blk_ptr))
        {
            write_block.write_data(zoneOutStream, blk_ptr);
            blk_ptr = 0;
            write_block.clear();
        }

        block_index++;
    }
    if (blk_ptr != 0)
    {
        write_block.write_data(zoneOutStream, blk_ptr);
    }
    yearDataStream.close();
    zoneOutStream.close();
}

/**
 * @brief Debug function to read zone map
 * This function reads the zone map from disk and prints it to the console
 * @param block_size
 */
void readZonemap(int block_size)
{
    std::ifstream zoneInStream("data/zone_maps/year_zones.dat", std::ios::in | std::ios::binary);
    int num_records_per_block = block_size / ColumnSizeConstants::year;
    Block<Zone<ColumnTypeConstants::year>> zoneBlock = Block<Zone<ColumnTypeConstants::year>>(block_size);
    std::cout << zoneBlock.block_data.size() << '\n';
    int pos = 0;
    while (!zoneInStream.eof())
    {
        zoneBlock.read_data(zoneInStream, pos, false);
        for (int i = 0; i < zoneBlock.block_data.size(); i++)
        {
            Zone<ColumnTypeConstants::year> zone = zoneBlock.block_data[i];
            std::cout << zoneBlock.block_data[i].getMin() << " " << zoneBlock.block_data[i].getMax() << " " << zoneBlock.block_data[i].getBlockId() << '\n';
        }
        pos += zoneBlock.block_data.size();
    }

    zoneInStream.close();
}