#if !defined(FILTER_H)
#define FILTER_H


template <class ReadPosition, class WritePosition, typename T>
class Filter
{
private:
    // position file name // if position file is nullptr then scan entire column // Read position into an object of Position / GroupBy Position
    // column file name
    // List of predicates
    // Save position file name along with groupby attribute? // Save into Position or GroupBy position
    
    // Instantiate Blocks. Move to the position and compare predicate. If yes save position in block.add_data
    // 1 block to read positions, 1 block to read column data, 1 block to write position
    

public:
    Filter();
    ~Filter();

    void process_filter(); // Use the templates here
};

// Filter::Filter()
// {
// }

// Filter::~Filter()
// {
// }

// class SortedFilter : public Filter
// {

// };

// class ZoneMapFilter : public Filter
// {

// };


#endif // FILTER_H
