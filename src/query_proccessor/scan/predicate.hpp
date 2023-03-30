#if !defined(PREDICATE_H)
#define PREDICATE_H

#include <string>
#include <vector>

template <typename T>
class Predicate
{
protected:
    std::string op;
    T value;

public:
    Predicate();
    Predicate(std::string op, T value);
    ~Predicate();
    virtual bool evaluate_expr(T lhs) = 0;
    virtual void display() = 0;
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
Predicate<T>::~Predicate()
{
}

template <typename T>
class AtomicPredicate : public Predicate<T>
{
public:
    AtomicPredicate(std::string op, T value);
    bool evaluate_expr(T lhs);
    void display();
    T get_value();
    std::string get_operator();
};

template <typename T>
AtomicPredicate<T>::AtomicPredicate(std::string op, T value) : Predicate<T>(op, value) {}

template <typename T>
bool AtomicPredicate<T>::evaluate_expr(T lhs)
{
    // std::cout << lhs << " " << this->op << " " << this->value <<'\n';
    if (this->op == "=")
        return lhs == this->value;
    if (this->op == "<")
        return lhs < this->value;
    if (this->op == ">")
        return lhs > this->value;
    if (this->op == "<=")
        return lhs <= this->value;
    if (this->op == ">=")
        return lhs >= this->value;
    if (this->op == "!=")
        return lhs != this->value;

    return false;
}

template <typename T>
void AtomicPredicate<T>::display()
{
    std::cout << "LHS"
              << " " << this->op << " " << this->value;
}

template <typename T>
T AtomicPredicate<T>::get_value()
{
    return this->value;
}

template <typename T>
std::string AtomicPredicate<T>::get_operator()
{
    return this->op;
}

template <typename T>
class CompundPredicate : public Predicate<T>
{
protected:
    std::vector<Predicate<T> *> predicates;

public:
    CompundPredicate(std::vector<Predicate<T> *> predicates);
    virtual bool evaluate_expr(T lhs) = 0;
    virtual void display() = 0;
};

template <typename T>
CompundPredicate<T>::CompundPredicate(std::vector<Predicate<T> *> predicates)
{
    this->predicates = predicates;
}

template <typename T>
class OrPredicate : public CompundPredicate<T>
{
public:
    bool evaluate_expr(T lhs);
    void display();
    OrPredicate(std::vector<Predicate<T> *> predicates);
};

template <typename T>
OrPredicate<T>::OrPredicate(std::vector<Predicate<T> *> predicates) : CompundPredicate<T>(predicates) {}

template <typename T>
bool OrPredicate<T>::evaluate_expr(T lhs)
{
    for (size_t i = 0; i < this->predicates.size(); ++i)
    {
        if (this->predicates[i]->evaluate_expr(lhs))
            return true;
    }
    return false;
}

template <typename T>
void OrPredicate<T>::display()
{
    for (size_t i = 0; i < this->predicates.size(); ++i)
    {
        std::cout << "(";
        this->predicates[i]->display();
        std::cout << ")";
        if (i < this->predicates.size() - 1)
            std::cout << " OR ";
    }
}

template <typename T>
class AndPredicate : public CompundPredicate<T>
{
public:
    AndPredicate(std::vector<Predicate<T> *> predicates);
    bool evaluate_expr(T lhs);
    void display();
};

template <typename T>
AndPredicate<T>::AndPredicate(std::vector<Predicate<T> *> predicates) : CompundPredicate<T>(predicates) {}

template <typename T>
bool AndPredicate<T>::evaluate_expr(T lhs)
{
    for (size_t i = 0; i < this->predicates.size(); ++i)
    {
        if (!this->predicates[i]->evaluate_expr(lhs))
            return false;
    }
    return true;
}

template <typename T>
void AndPredicate<T>::display()
{
    for (size_t i = 0; i < this->predicates.size(); ++i)
    {
        std::cout << "(";
        this->predicates[i].display();
        std::cout << ")";
        if (i < this->predicates.size() - 1)
            std::cout << " AND ";
    }
}

#endif // PREDICATE_H
