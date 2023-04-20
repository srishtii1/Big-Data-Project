/**
 * @file query.hpp
 * @author Srishti
 * @brief Simple Query Abstraction
 * @version 0.1
 * @date 2023-04-20
 *
 * @copyright Copyright (c) 2023
 *
 */
#ifndef QUERY_H
#define QEURY_H

/**
 * @brief struct for Query
 */
struct Query
{
    /**
     * @property year1: beginning year
     * @property year2: ending year
     * @property city: city to filter by
     */
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