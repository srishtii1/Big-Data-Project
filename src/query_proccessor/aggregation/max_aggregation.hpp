/**
 * @file max_aggregation.hpp
 * @author Atul
 * @brief Maximum Aggregation header file
 * @version 0.1
 * @date 2023-04-20
 *
 * @copyright Copyright (c) 2023
 *
 */
#if !defined(MAX_AGGREGATE_H)
#define MAX_AGGREGATE_H

#include "aggregate_function.hpp"
#include <limits>
#include <algorithm>

/**
 * @brief Maximum Aggregation child class
 * @tparam T
 */
template <typename T>
class MaxAggregation : public AggregationFunction<T>
{
private:
    /**
     * @property count: number of values aggregated
     * @property value: value of generic type
     */
    int count;
    T value;

public:
    MaxAggregation();
    void add_value(T value);
    T compute_value();
};

/**
 * @brief Construct a new Max Aggregation< T>:: Max Aggregation object
 * @tparam T
 */
template <typename T>
MaxAggregation<T>::MaxAggregation()
{
    this->count = 0;
    this->value = std::numeric_limits<T>::min();
}

/**
 * @brief Add new value to be aggregated
 * This function takes as input a new value to be aggregated and calculates and stores the new maximum based on it
 * @tparam T
 * @param value: value of generic type
 */
template <typename T>
void MaxAggregation<T>::add_value(T value)
{
    ++this->count;
    this->value = std::max(this->value, value);
}

/**
 * @brief Method to return to aggregated (maximum) value
 * @tparam T
 * @return T
 */
template <typename T>
T MaxAggregation<T>::compute_value()
{
    return this->value;
}

#endif // MAX_AGGREGATE_H
