#if !defined(MIN_AGGREGATE_H)
#define MIN_AGGREGATE_H

#include "aggregate_function.hpp"
#include <limits>
#include <algorithm>

template <typename T>
class MinAggregation : public AggregationFunction<T>
{
private:
    int count;
    T value;
public:
    MinAggregation();
    void add_value(T value);
    T compute_value();
};

template <typename T>
MinAggregation<T>::MinAggregation()
{
    this->count = 0;
    this->value = std::numeric_limits<T>::max();
}

template<typename T>
void MinAggregation<T>::add_value(T value)
{
    ++this->count;
    this->value = std::min(this->value, value);
}

template<typename T>
T MinAggregation<T>::compute_value()
{
    return this->value;
}


#endif // MIN_AGGREGATE_H
