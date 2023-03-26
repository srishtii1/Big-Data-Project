#if !defined(AGGREGATE_H)
#define AGGREGATE_H

template <typename T>
class AggregationFunction
{
public:
    AggregationFunction();
    virtual void compute_value(T value) = 0;
    
};

#endif // AGGREGATE_H
