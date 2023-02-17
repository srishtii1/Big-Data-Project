#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include<cstdio>
#include<unordered_map>

#include "constants.hpp"


void read_csv(){
    std::ifstream input_file;
    input_file.open("data/SingaporeWeather.csv", std::ios::in);

    std::unordered_map<std::string, std::ofstream*> file_map;

    for (auto it=FileNameConstants::file_names.begin(); it != FileNameConstants::file_names.end(); ++it)
    {
        std::ofstream* output_file = new std::ofstream("data/column_store/" + it->second, std::ios::out | std::ios::binary);
        file_map[it->first] = output_file;
    }

    std::vector<std::string> row;

    std::string line, word;
    
    int counter = 0;

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

        if (counter <= 1) continue; // Skip header

        // Process Timestamp
        auto timestamp = row[1];
        // YYYY-MM-DD HH:MM
        // TODO: Discuss whether to use stringstream for this
        unsigned short year = (unsigned short) std::stoi(timestamp.substr(0, 4));
        unsigned short month = (unsigned short) std::stoi(timestamp.substr(5, 2));
        unsigned short day = (unsigned short) std::stoi(timestamp.substr(8, 2));

        unsigned short hour = (unsigned short) std::stoi(timestamp.substr(11, 2));
        unsigned short minute = (unsigned short) std::stoi(timestamp.substr(14, 2));

        file_map["year"]->write((char *) &year, sizeof(ColumnSizeConstants::year));
        file_map["month"]->write((char *) &month, sizeof(ColumnSizeConstants::month));
        file_map["day"]->write((char *) &day, sizeof(ColumnSizeConstants::day));

        unsigned short time = (minute == 30) ? 2 * hour + 1 : 2 * hour;

        file_map["time"]->write((char *) &time, sizeof(ColumnSizeConstants::time));

        // Process Station
        auto station = row[2];
        bool station_encoded = station == "Changi" ? false : true;
        file_map["city"]->write((char *) &station_encoded, sizeof(ColumnSizeConstants::city));

        // Process Temperature
        float temperature = std::stof(row[3]);
        file_map["temperature"]->write((char *) &temperature, sizeof(ColumnSizeConstants::temperature));

        // Process Humidity
        float humidity = std::stof(row[3]);
        file_map["humidity"]->write((char *) &humidity, sizeof(ColumnSizeConstants::humidity));

    }

    for (auto it=file_map.begin(); it != file_map.end(); ++it)
    {
        it->second->close();
    }
    
    // return;
}

int main() 
{
    read_csv();
    return 0;
}