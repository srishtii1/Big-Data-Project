#if !defined(PREDICATE_H)
#define PREDICATE_H

#include <string>
#include <vector>

template <typename T>
class Predicate
{
private:
    std::string op;
    T value;
public:
    Predicate();
    Predicate(std::string op, T value);
    ~Predicate();
    bool evaluate_expr(T lhs);
};

template <typename T>
Predicate<T>::Predicate()
{
}

template <typename T>
Predicate<T>::Predicate(std::string op, T value)

{
    this->op = op;
    this->value = value;
}

template <typename T>
bool Predicate<T>::evaluate_expr(T lhs)
{
    if (this->op == "=") return lhs == this->value;
    if (this->op == "<") return lhs < this->value;
    if (this->op == ">") return lhs > this->value;
    if (this->op == "<=") return lhs <= this->value;
    if (this->op == ">=") return lhs >= this->value;
    if (this->op == "!=") return lhs != this->value;
    
    return false;
}

template <typename T>
Predicate<T>::~Predicate()
{
}


template <typename T>
class CompundPredicate : public Predicate<T>
{
    protected:
        std::vector<Predicate<T>> predicates;
    public:
        CompundPredicate(std::vector<Predicate<T>> predicates);
        virtual bool evaluate_expr(T lhs) = 0;
    
};

template <typename T>
CompundPredicate<T>::CompundPredicate(std::vector<Predicate<T>> predicates)
{
    this->predicates = predicates;
}

template <typename T>
class OrPredicate : public CompundPredicate<T>
{
    public:
        bool evaluate_expr(T lhs);
        OrPredicate(std::vector<Predicate<T>> predicates);
};

template <typename T>
OrPredicate<T>::OrPredicate(std::vector<Predicate<T>> predicates) : CompundPredicate<T>(predicates) {}

template <typename T>
bool OrPredicate<T>::evaluate_expr(T lhs)
{
    for (size_t i=0; i<this->predicates.size(); ++i)
    {
        if (this->predicates[i].evaluate_expr(lhs)) return true;
    }
    return false;
}

template <typename T>
class AndPredicate : public CompundPredicate<T>
{
    public:
        AndPredicate(std::vector<Predicate<T>> predicates);
        bool evaluate_expr(T lhs);
};

template <typename T>
AndPredicate<T>::AndPredicate(std::vector<Predicate<T>> predicates) : CompundPredicate<T>(predicates) {}

template <typename T>
bool AndPredicate<T>::evaluate_expr(T lhs)
{
    for (size_t i=0; i<this->predicates.size(); ++i)
    {
        if (!this->predicates[i].evaluate_expr(lhs)) return false;
    }
    return true;
}




#endif // PREDICATE_H
