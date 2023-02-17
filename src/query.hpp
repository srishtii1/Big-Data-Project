#ifndef QUERY_H
#define QEURY_H

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

#endif