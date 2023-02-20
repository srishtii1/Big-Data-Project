#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include<cstring>
#include <cassert> 

#include "src/preprocessing.hpp"
#include "src/query.hpp"

Query parse_query(std::string matric_no)
{
    int year = (int)(matric_no[matric_no.size() - 2]) - 48;

    bool city = ((int)matric_no[matric_no.size() - 2] - 48) % 2;

    return Query(year, city);
}


int main(int argc, char** argv) 
{
    try
    {
        assert (argc == 2);

        std::string matriculation_number = argv[1];

        Query query = parse_query(matriculation_number);
        std::cout << "Query: " << query.year1 << " " << query.year2<<" "<<query.city << std::endl;
        preprocess_csv();

    }
    
    catch (const std::exception &exc)
    {
        std::cerr << "Exception " << exc.what();
    }
    return 0;
}