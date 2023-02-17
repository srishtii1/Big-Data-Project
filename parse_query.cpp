#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>

using namespace std;

struct Query {
    int year1;
    int year2;
    char location[20];
};

Query parse_query(string matric_no)
{
    Query query;
    matric_no.pop_back();
    int last_digit = matric_no.back();
    int second_last_digit = matric_no[8];
    query.year1 = 2000 + last_digit;
    query.year2 = 2010 + last_digit;
    if (second_last_digit % 2 == 0) {
        strcpy(query.location, "Changi");
    } else {
        strcpy(query.location, "Paya Lebar");
    }
    return query;
}

int main() {
    Query query = parse_query("U1922129L");
    printf(query.location, " %d %d", query.year1, query.year2);
    return 0;
}