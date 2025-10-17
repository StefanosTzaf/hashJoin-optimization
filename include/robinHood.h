#include <vector>
#include <optional>


template <class keyT, class valueT>
class hashNode{
public:
    keyT   key;
    valueT value;
    bool   isOccupied;

    hashNode() : isOccupied(false) {

    }
};

template <class keyT, class valueT>
class RobinHoodHashTable {

private:
    std::vector<hashNode<keyT, valueT>> table;
    size_t capacity;
    size_t size;

    void rehash();
    //the second const declares that the function does not modify any member variables (read-only function)
    size_t hashFunction(const keyT& key) const;

public:
    //constructor
    RobinHoodHashTable(size_t initialCapacity = 16);
    bool hashInsert(const keyT& key, const valueT& value);
    bool hashSearch(const keyT& key, valueT& value) const;

    // if not found, return std::nullopt 
    std::optional<valueT> hashSearch(const keyT& key) const;

    size_t getSize() const {
        return size;
    }
};