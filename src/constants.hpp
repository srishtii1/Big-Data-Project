#ifndef CONSTANTS_H
#define CONSTANTS_H

#include<string>
#include<unordered_map>

namespace FileNameConstants
{
    const std::string year = "year_encoded.dat";
    const std::string month = "month_encoded.dat";
    const std::string day = "day_encoded.dat";
    const std::string city = "city_encoded.dat";
    const std::string time = "time_encoded.dat";
    const std::string temperature = "temperature_encoded.dat";
    const std::string humidity = "humidity_encoded.dat";

    const std::unordered_map<std::string, std::string> file_names = 
    {
        {"year", year},
        {"month", month},
        {"day", day}, 
        {"city", city}, 
        {"time", time}, 
        {"temperature", temperature}, 
        {"humidity", humidity}
    };

}

namespace ColumnSizeConstants
{
    const int year = sizeof(__int8);
    const int month = sizeof(__int8);
    const int day = sizeof(__int8);

    const int city = sizeof(bool);

    const int time = sizeof(__int8);
    const int temperature = sizeof(float);
    const int humidity = sizeof(float);
}

namespace ProgramConstants
{
    const int num_columns = 476922;
}


#endif