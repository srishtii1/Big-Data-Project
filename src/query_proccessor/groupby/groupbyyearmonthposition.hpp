#if !defined(GROUPBYYEARMONTHPOS_H)
#define GROUPBYYEARMONTHPOS_H

#include <iostream>
#include "../../constants.hpp"

struct GroupByYearMonthPosition
{
    ColumnTypeConstants::position position;
    char key[8];

    GroupByYearMonthPosition() {}

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

    friend std::ostream &operator<<(std::ostream &o, const GroupByYearMonthPosition &p)
    {
        return o << "Position " << p.position << std::endl;
    }
};

#endif