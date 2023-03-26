#if !defined(POSITION_H)
#define POSITION_H


struct Position
{
    int position;
};

struct GroupByYearMonthPosition : public Position
{
    uint8_t month;
    uint16_t year;
};



#endif // POSITION_H
