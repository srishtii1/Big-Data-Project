/**
 * @file min_aggregation.hpp
 * @author Atul
 * @brief Minimum Aggregation header file
 * @version 0.1
 * @date 2023-04-20
 *
 * @copyright Copyright (c) 2023
 *
 */
#if !defined(MIN_AGGREGATE_H)
#define MIN_AGGREGATE_H

#include "aggregate_function.hpp"
#include <limits>
#include <algorithm>

/**
 * @brief Minimum Aggregation child class
 * @tparam T
 */
template <typename T>
class MinAggregation : public AggregationFunction<T>
{
private:
    /**
     * @property count: number of values aggregated
     * @property value: value of generic type
     */
    int count;
    T value;

public:
    MinAggregation();
    void add_value(T value);
    T compute_value();
};

/**
 * @brief Construct a new Min Aggregation< T>:: Min Aggregation object
 * @tparam T
 */
template <typename T>
MinAggregation<T>::MinAggregation()
{
    this->count = 0;
    this->value = std::numeric_limits<T>::max();
}

/**
 * @brief Add new value to be aggregated
 * This function takes as input a new value to be aggregated and calculates and stores the new minumum based on it
 * @tparam T
 * @param value: value of generic type
 */
template <typename T>
void MinAggregation<T>::add_value(T value)
{
    ++this->count;
    this->value = std::min(this->value, value);
}

/**
 * @brief Method to return to aggregated (minimum) value
 * @tparam T
 * @return T
 */
template <typename T>
T MinAggregation<T>::compute_value()
{
    return this->value;
}

#endif // MIN_AGGREGATE_H
