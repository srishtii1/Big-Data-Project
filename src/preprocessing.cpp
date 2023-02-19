#include "preprocessing.hpp"

void preprocess_csv()
{
    std::ifstream input_file;
    input_file.open("../data/SingaporeWeather.csv", std::ios::in);
    std::cout << "Opened file" << std::endl;
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
        std::cout << "File good!" << std::endl;
        row.clear();
        getline(input_file, line);
        std::stringstream s(line);

        ++counter;

        while (getline(s, word, ','))
        {
            std::cout << word << std::endl;
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
        std::cout << "Outputted year" << std::endl;
        file_map["month"]->write((char *) &month, sizeof(ColumnSizeConstants::month));
        file_map["day"]->write((char *) &day, sizeof(ColumnSizeConstants::day));

        // encode the time of the day as one unsigned short instead of 2; since time is measured at the granularity of every 30 minutes
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
    
}
