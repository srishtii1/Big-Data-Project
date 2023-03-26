#if !defined(MAX_AGGREGATE_H)
#define MAX_AGGREGATE_H

#include "aggregate_function.hpp"
#include <limits>

template <typename T>
class MaxAggregation : public AggregationFunction<T>
{
private:
    int count;
    T value;
public:
    MaxAggregation();
    ~MaxAggregation();
    void compute_value(T value);
};

template <typename T>
MaxAggregation<T>::MaxAggregation()
{
    this->count = 0;
    this->value = std::numeric_limits<T>::min();
}

template<typename T>
void MaxAggregation<T>::compute_value(T value)
{
    ++this->count;
    this->value = max(this->value, value);
}


#endif // MAX_AGGREGATE_H
