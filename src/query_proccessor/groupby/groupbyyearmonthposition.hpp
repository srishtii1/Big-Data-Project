/**
 * @file groupbyyearmonthposition.hpp
 * @author Atul
 * @brief Header file for custom "position" type which also stores the GroupBy key
 * @version 0.1
 * @date 2023-04-20
 *
 * @copyright Copyright (c) 2023
 *
 */
#if !defined(GROUPBYYEARMONTHPOS_H)
#define GROUPBYYEARMONTHPOS_H

#include <iostream>
#include "../../constants.hpp"

/**
 * @brief struct to define custom "position" type
 */
struct GroupByYearMonthPosition
{
    /**
     * @property position: position within file
     * @property key: GroupBy key constructed from corresponding year and month
     */
    ColumnTypeConstants::position position;
    char key[8];

    /**
     * @brief Construct a new Group By Year Month Position object
     */
    GroupByYearMonthPosition() {}

    /**
     * @brief Construct a new Group By Year Month Position object
     * @param position: position within file
     * @param year: year in GroupBy key
     * @param month: month in GroupBy key
     */
    GroupByYearMonthPosition(ColumnTypeConstants::position position, uint16_t year, uint8_t month)
    {
        this->position = position;
        std::string year_string = std::to_string(year);
        std::string month_string = std::to_string(month);
        if (month_string.size() == 1)
            month_string = "0" + month_string;
        for (int i = 0; i < year_string.size(); ++i)
        {
            this->key[i] = year_string[i];
        }
        this->key[4] = '-';
        for (int i = 0; i < month_string.size(); ++i)
        {
            this->key[5 + i] = month_string[i];
        }
        this->key[7] = '\0';
    }

    /**
     * @brief Debug function to print custom "position" type
     *
     * @param o: output stream to print to
     * @param p: position object to be printed
     * @return std::ostream&
     */
    friend std::ostream &operator<<(std::ostream &o, const GroupByYearMonthPosition &p)
    {
        return o << "Position " << p.position << std::endl;
    }
};

#endif