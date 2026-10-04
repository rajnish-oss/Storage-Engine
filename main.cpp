#include "db.hpp"
#include <iostream>

int main() {
    DB<std::string, std::string> db(3, 3); // Create DB with specific limits
    db.put("a", "apple");                 // Add key-value pair
    db.del("a");                           // Delete a key
    auto val = db.get("a");                // Retrieve value given a key
    return 0;
}