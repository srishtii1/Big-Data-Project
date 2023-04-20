#include "query_processing.hpp"
#include "query_proccessor/scan/predicate.hpp"
#include "query_proccessor/scan/filter.hpp"
#include "query_proccessor/scan/binary_search_filter.hpp"
#include "query_proccessor/scan/zonemap_filter.hpp"
#include "query_proccessor/groupby/groupby.hpp"

#include "query_proccessor/projection.hpp"

#include <map>
#include <cstring>
#include <iostream>

/**
 * @brief Construct a new Query Processor:: Query Processor object
 * @param block_size: desired block size
 */
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

/**
 * @brief Method to process query of required type
 * @param matric_num: matriculation number in query
 * @param year1: first year to filter by
 * @param year2: second year to filter by
 * @param city: station to be filtered by
 * @param yearFilterType: filter type to be used: BinarySearch, ZoneMap, or Filter
 */
void QueryProcessor::process_query(std::string matric_num, uint16_t year1, uint16_t year2, bool city, std::string yearFilterType)
{

    // Filer Year
    if (yearFilterType == "BinarySearch")
    {
        AtomicPredicate<ColumnTypeConstants::year> p1 = AtomicPredicate<ColumnTypeConstants::year>("=", year1);
        AtomicPredicate<ColumnTypeConstants::year> p2 = AtomicPredicate<ColumnTypeConstants::year>("=", year2);

        BinarySearchFilter<ColumnTypeConstants::year> year_filer_binary_search = BinarySearchFilter<ColumnTypeConstants::year>("data/column_store/temp/positions.dat", "data/column_store/temp/temp1.dat", "data/column_store/year_encoded.dat", this->block_size);
        year_filer_binary_search.process_filter({p1, p2}, 0, ProgramConstants::num_rows, true);
    }
    else if (yearFilterType == "ZoneMap")
    {
        AtomicPredicate<ColumnTypeConstants::year> lowerCutOffYear1 = AtomicPredicate<ColumnTypeConstants::year>("<=", year1);
        AtomicPredicate<ColumnTypeConstants::year> upperCutOffYear1 = AtomicPredicate<ColumnTypeConstants::year>(">=", year1);
        std::pair<AtomicPredicate<ColumnTypeConstants::year>, AtomicPredicate<ColumnTypeConstants::year>> year1Cutoff = std::make_pair(lowerCutOffYear1, upperCutOffYear1);

        AtomicPredicate<ColumnTypeConstants::year> lowerCutOffYear2 = AtomicPredicate<ColumnTypeConstants::year>("<=", year2);
        AtomicPredicate<ColumnTypeConstants::year> upperCutOffYear2 = AtomicPredicate<ColumnTypeConstants::year>(">=", year2);
        std::pair<AtomicPredicate<ColumnTypeConstants::year>, AtomicPredicate<ColumnTypeConstants::year>> year2Cutoff = std::make_pair(lowerCutOffYear2, upperCutOffYear2);

        ZonemapFilter<ColumnTypeConstants::year> year_filter_zonemap = ZonemapFilter<ColumnTypeConstants::year>("data/column_store/temp/positions.dat", "data/column_store/temp/temp1.dat", "data/column_store/year_encoded.dat", "data/zone_maps/year_zones.dat", this->block_size);
        year_filter_zonemap.process_filter({year1Cutoff, year2Cutoff}, true);
    }
    else
    {
        AtomicPredicate<ColumnTypeConstants::year> *p1 = new AtomicPredicate<ColumnTypeConstants::year>("=", year1);
        AtomicPredicate<ColumnTypeConstants::year> *p2 = new AtomicPredicate<ColumnTypeConstants::year>("=", year2);
        OrPredicate<ColumnTypeConstants::year> orPred = OrPredicate<ColumnTypeConstants::year>({p1, p2});
        Filter<ColumnTypeConstants::year> year_filter = Filter<ColumnTypeConstants::year>("data/column_store/temp/positions.dat", "data/column_store/temp/temp1.dat", "data/column_store/year_encoded.dat", this->block_size);
        year_filter.process_filter(orPred, true);
    }

    // Filter City
    AtomicPredicate<ColumnTypeConstants::city> p3 = AtomicPredicate<ColumnTypeConstants::city>("=", city);
    Filter<ColumnTypeConstants::city> city_filter = Filter<ColumnTypeConstants::city>("data/column_store/temp/temp1.dat", "data/column_store/temp/temp2.dat", "data/column_store/city_encoded.dat", this->block_size);
    city_filter.process_filter(p3);

    // Save Group By Key for the required positions along with posiiton
    GroupBy groupby_year_month = GroupBy(this->block_size);
    groupby_year_month.save_groupby_key("data/column_store/temp/temp2.dat", "data/column_store/temp/temp3.dat", "data/column_store/raw_timestamp_encoded.dat");

    // Perform aggregation and filter rows for temperature
    std::map<std::string, ColumnTypeConstants::temperature> min_temp;
    std::map<std::string, ColumnTypeConstants::temperature> max_temp;
    groupby_year_month.save_aggregation("data/column_store/temp/temp3.dat", "data/column_store/temp/max_temp.dat", "data/column_store/temp/min_temp.dat", "data/column_store/temperature_encoded.dat", min_temp, max_temp);

    // Perform aggregation and filter rows for humidity
    std::map<std::string, ColumnTypeConstants::humidity> min_humid;
    std::map<std::string, ColumnTypeConstants::humidity> max_humid;
    groupby_year_month.save_aggregation("data/column_store/temp/temp3.dat", "data/column_store/temp/max_humid.dat", "data/column_store/temp/min_humid.dat", "data/column_store/humidity_encoded.dat", min_humid, max_humid);

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