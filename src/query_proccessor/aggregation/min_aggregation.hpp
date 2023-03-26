#if !defined(MIN_AGGREGATE_H)
#define MIN_AGGREGATE_H

#include "aggregate_function.hpp"
#include <limits>

template <typename T>
class MinAggregation : public AggregationFunction<T>
{
private:
    int count;
    T value;
public:
    MinAggregation();
    ~MinAggregation();
    void compute_value(T value);
};

template <typename T>
MinAggregation<T>::MinAggregation()
{
    this->count = 0;
    this->value = std::numeric_limits<T>::max();
}

template<typename T>
void MinAggregation<T>::compute_value(T value)
{
    ++this->count;
    this->value = min(this->value, value);
}


#endif // MIN_AGGREGATE_H
