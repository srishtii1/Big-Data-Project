/**
 * @file aggregate_function.hpp
 * @author Atul
 * @brief Aggregation Function header file
 * @version 0.1
 * @date 2023-04-20
 *
 * @copyright Copyright (c) 2023
 *
 */
#if !defined(AGGREGATE_H)
#define AGGREGATE_H

/**
 * @brief Generic Abstract (Virtual) Aggregation Function Class
 * @tparam T: generic type
 */
template <typename T>
class AggregationFunction
{
public:
    virtual void add_value(T value) = 0;
    virtual T compute_value() = 0;
};

#endif // AGGREGATE_H
