#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include<cstring>
#include <cassert> 


struct Query {
    int year1;
    int year2;
    bool city;

    Query(int year, bool city) 
    {
        this->year1 = 2000 + year;
        this->year2 = 2010 + year;
        this->city = city;
    }
};

Query parse_query(std::string matric_no)
{
    int year = (int)(matric_no[matric_no.size() - 2]) - 48;

    bool city = ((int)matric_no[matric_no.size() - 2] - 48) % 2;

    return Query(year, city);
}

// int main() {
//     Query query = parse_query("U1922129L");
//     printf(query.location, " %d %d", query.year1, query.year2);
//     return 0;
// }

int main(int argc, char** argv) 
{
    assert (argc == 2);

    std::string matriculation_number = argv[1];

    Query query = parse_query(matriculation_number);

    return 0;
}