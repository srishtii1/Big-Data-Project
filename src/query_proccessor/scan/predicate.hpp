/**
 * @file predicate.hpp
 * @author Atul
 * @brief Header file for Predicate class
 * @version 0.1
 * @date 2023-04-20
 *
 * @copyright Copyright (c) 2023
 *
 */
#if !defined(PREDICATE_H)
#define PREDICATE_H

#include <string>
#include <vector>

/**
 * @brief Generic (Parent) Predicate class to define predicates for filtering
 * @tparam T
 */
template <typename T>
class Predicate
{
protected:
    /**
     * @property op: the operation to perform
     * @property value: the generic constant value used in the predicate
     */
    std::string op;
    T value;

public:
    Predicate();
    Predicate(std::string op, T value);
    ~Predicate();
    virtual bool evaluate_expr(T lhs) = 0;
    virtual void display() = 0;
};

/**
 * @brief Construct a new Predicate< T>:: Predicate object
 * @tparam T
 */
template <typename T>
Predicate<T>::Predicate()
{
}

/**
 * @brief Construct a new Predicate< T>:: Predicate object
 * @tparam T
 * @param op: operation to perform
 * @param value: value used in predicate
 */
template <typename T>
Predicate<T>::Predicate(std::string op, T value)
{
    this->op = op;
    this->value = value;
}

/**
 * @brief Destroy the Predicate< T>:: Predicate object
 * @tparam T
 */
template <typename T>
Predicate<T>::~Predicate()
{
}

/**
 * @brief (Child) class to define an atomic predicate
 * @tparam T
 */
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

/**
 * @brief Construct a new Atomic Predicate< T>:: Atomic Predicate object
 * @tparam T
 * @param op: operation to perform
 * @param value: value used in predicate
 */
template <typename T>
AtomicPredicate<T>::AtomicPredicate(std::string op, T value) : Predicate<T>(op, value) {}

/**
 * @brief Method to evaluate the expression with a given value
 * @tparam T
 * @param lhs: value to be evaluated against
 * @return bool
 */
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

/**
 * @brief Debug method to display the predicate operation
 * @tparam T
 */
template <typename T>
void AtomicPredicate<T>::display()
{
    std::cout << "LHS"
              << " " << this->op << " " << this->value;
}

/**
 * @brief Get constant value involved in the predicate
 * @tparam T
 * @return T
 */
template <typename T>
T AtomicPredicate<T>::get_value()
{
    return this->value;
}

/**
 * @brief Get operator involved in the predicate
 * @tparam T
 */
template <typename T>
std::string AtomicPredicate<T>::get_operator()
{
    return this->op;
}

/**
 * @brief (Parent) class to define compound predicate (multiple predicates), inheriting from Predicate
 * @tparam T
 */
template <typename T>
class CompundPredicate : public Predicate<T>
{
protected:
    /**
     * @property predicates: vector of predicates to filter by
     */
    std::vector<Predicate<T> *> predicates;

public:
    CompundPredicate(std::vector<Predicate<T> *> predicates);
    virtual bool evaluate_expr(T lhs) = 0;
    virtual void display() = 0;
};

/**
 * @brief Construct a new Compund Predicate< T>:: Compund Predicate object
 * @tparam T
 * @param predicates: vector of predicate to filter by
 */
template <typename T>
CompundPredicate<T>::CompundPredicate(std::vector<Predicate<T> *> predicates)
{
    this->predicates = predicates;
}

/**
 * @brief (Child) class to define an OR operation with several predicates, inheriting from CompoundPredicate
 * @tparam T
 */
template <typename T>
class OrPredicate : public CompundPredicate<T>
{
public:
    bool evaluate_expr(T lhs);
    void display();
    OrPredicate(std::vector<Predicate<T> *> predicates);
};

/**
 * @brief Construct a new Or Predicate< T>:: Or Predicate object
 * @tparam T
 * @param predicates: vector of predicate to filter by
 */
template <typename T>
OrPredicate<T>::OrPredicate(std::vector<Predicate<T> *> predicates) : CompundPredicate<T>(predicates) {}

/**
 * @brief Method to evalute OR expression
 * @tparam T
 * @param lhs: value to be evaluated against
 * @return bool
 */
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

/**
 * @brief Debug method to print OR expression
 * @tparam T
 */
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

/**
 * @brief (Child) class to define an AND operation with several predicates, inheriting from CompoundPredicate
 * @tparam T
 */
template <typename T>
class AndPredicate : public CompundPredicate<T>
{
public:
    AndPredicate(std::vector<Predicate<T> *> predicates);
    bool evaluate_expr(T lhs);
    void display();
};

/**
 * @brief Construct a new And Predicate< T>:: And Predicate object
 * @tparam T
 * @param predicates: vector of predicate to filter by
 */
template <typename T>
AndPredicate<T>::AndPredicate(std::vector<Predicate<T> *> predicates) : CompundPredicate<T>(predicates) {}

/**
 * @brief  Method to evalute AND expression
 * @tparam T
 * @param lhs: value to be evaluted against
 * @return bool
 */
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

/**
 * @brief Debug method to print AND expression
 * @tparam T
 */
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
