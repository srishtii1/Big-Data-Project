#ifndef CONSTANTS_H
#define CONSTANTS_H

#include <string>
#include <unordered_map>
#include <map>
#include <any>

/**
 * @brief Contants file
 * This file defines different namespaces under which relevant program constants are defined.
 */

namespace FileNameConstants
{
    const std::string year = "year_encoded.dat";
    const std::string month = "month_encoded.dat";
    const std::string day = "day_encoded.dat";
    const std::string city = "city_encoded.dat";
    const std::string time = "time_encoded.dat";
    const std::string temperature = "temperature_encoded.dat";
    const std::string humidity = "humidity_encoded.dat";
    const std::string raw_timestamp = "raw_timestamp_encoded.dat";

    const std::string positions = "temp/positions.dat";

    const std::unordered_map<std::string, std::string> file_names =
        {
            {"year", year},
            {"month", month},
            {"day", day},
            {"city", city},
            {"time", time},
            {"temperature", temperature},
            {"humidity", humidity},
            {"raw_timestamp", raw_timestamp},
            {"positions", positions}};

}

namespace ColumnTypeConstants
{
    typedef uint16_t year;
    typedef uint8_t month;
    typedef uint8_t day;

    typedef bool city;

    typedef uint16_t time;
    typedef time_t raw_timestamp;

    typedef float temperature;
    typedef float humidity;
    typedef uint32_t position;
    typedef uint32_t block_id;
}

namespace ColumnSizeConstants
{
    const int year = sizeof(ColumnTypeConstants::year);
    const int month = sizeof(ColumnTypeConstants::month);
    const int day = sizeof(ColumnTypeConstants::day);

    const int raw_timestamp = sizeof(ColumnTypeConstants::raw_timestamp);

    const int city = sizeof(ColumnTypeConstants::city);

    const int time = sizeof(ColumnTypeConstants::time);
    const int temperature = sizeof(ColumnTypeConstants::temperature);
    const int humidity = sizeof(ColumnTypeConstants::humidity);

    const int position = sizeof(ColumnTypeConstants::position);
    const int Zone = year + year + sizeof(ColumnTypeConstants::block_id);
}

namespace ProgramConstants
{
    const int num_rows = 476920;
}

#endif