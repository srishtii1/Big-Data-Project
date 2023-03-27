#if !defined(MAX_AGGREGATE_H)
#define MAX_AGGREGATE_H

#include "aggregate_function.hpp"
#include <limits>
#include <algorithm>

template <typename T>
class MaxAggregation : public AggregationFunction<T>
{
private:
    int count;
    T value;

public:
    MaxAggregation();
    void add_value(T value);
    T compute_value();
};

template <typename T>
MaxAggregation<T>::MaxAggregation()
{
    this->count = 0;
    this->value = std::numeric_limits<T>::min();
}

template<typename T>
void MaxAggregation<T>::add_value(T value)
{
    ++this->count;
    this->value = std::max(this->value, value);
}

template<typename T>
T MaxAggregation<T>::compute_value()
{
    return this->value;
}


#endif // MAX_AGGREGATE_H
