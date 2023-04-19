/**
* This class defines a zone of type T. Zones form the fundamental building blocks of the zonemap.
* Args: BlockId, Min, Max
**/
template <typename T>
class Zone{
    private:
        T min;
        T max;
        int block_id;
    public:
        Zone();
        Zone(int block_id, T min, T max);
        int getBlockId();
        T getMin();
        T getMax();
        void setBlockId(int block_id);
        void setMin(T min);
        void setMax(T max);
};

template <typename T>
inline Zone<T>::Zone()
{
    this->min = 0;
    this->max = 0;
    this->block_id = -1;
}

template <typename T>
inline Zone<T>::Zone(int block_id, T min, T max)
{
    this->min = min;
    this->max = max;
    this->block_id = block_id;
}

template <typename T>
inline T Zone<T>::getMin()
{
    return this->min;
}

template <typename T>
inline void Zone<T>::setMin(T min)
{
    this->min = min;
}

template <typename T>
inline T Zone<T>::getMax()
{
    return this->max;
}

template <typename T>
inline void Zone<T>::setMax(T max)
{
    this->max = max;
}

template <typename T>
inline int Zone<T>::getBlockId()
{
    return this->block_id;
}

