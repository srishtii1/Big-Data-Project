#include "preprocessing.hpp"

void preprocess_csv()
{
    std::ifstream input_file;
    
    // float x[100];
    // input_file.open("data/column_store/humidity_encoded.dat", std::ios::in | std::ios::binary);

    // input_file.read((char*)&x, sizeof(float) * 100);

    // for (int i=0; i<100; ++i)
    // {
    //     std::cout << x[i] << " ";
    // }
    // std::cout<<"\n";
    // input_file.close();

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
            std::cout << word << std::endl;
            row.push_back(word);
        }

        if (counter <= 1) continue; // Skip header

        // Process Timestamp
        auto timestamp = row[1];
        // YYYY-MM-DD HH:MM
        // TODO: Discuss whether to use stringstream for this
        __int8 year = (__int8) std::stoi(timestamp.substr(0, 4));
        __int8 month = (__int8) std::stoi(timestamp.substr(5, 2));
        __int8 day = (__int8) std::stoi(timestamp.substr(8, 2));

        __int8 hour = (__int8) std::stoi(timestamp.substr(11, 2));
        __int8 minute = (__int8) std::stoi(timestamp.substr(14, 2));

        file_map["year"]->write((char *) &year, (ColumnSizeConstants::year));
        file_map["month"]->write((char *) &month, (ColumnSizeConstants::month));
        file_map["day"]->write((char *) &day, (ColumnSizeConstants::day));

        // encode the time of the day as one unsigned short instead of 2; since time is measured at the granularity of every 30 minutes
        __int8 time = (minute == 30) ? 2 * hour + 1 : 2 * hour;

        file_map["time"]->write((char *) &time, (ColumnSizeConstants::time));

        // Process Station
        auto station = row[2];
        bool station_encoded = station == "Changi" ? false : true;
        file_map["city"]->write((char *) &station_encoded, (ColumnSizeConstants::city));

        // Process Temperature
        float temperature = std::stof(row[3]);
        file_map["temperature"]->write((char *) &temperature, (ColumnSizeConstants::temperature));

        // Process Humidity
        float humidity = std::stof(row[4]);
        file_map["humidity"]->write((char *) &humidity, (ColumnSizeConstants::humidity));

    }

    for (auto it=file_map.begin(); it != file_map.end(); ++it)
    {
        it->second->close();
    }

    input_file.close();
    
}
