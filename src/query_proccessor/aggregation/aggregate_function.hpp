#if !defined(AGGREGATE_H)
#define AGGREGATE_H

template <typename T>
class AggregationFunction
{
public:
    virtual void add_value(T value) = 0;
    virtual T compute_value() = 0;
    
};

#endif // AGGREGATE_H
