#include <iostream>

#include "main/kuzu.h"
using namespace kuzu::main;

int main() {
    try {
        std::cout << "Hello, World!" << std::endl;
        std::cout << "Printed from C++." << std::endl;
        std::cout << "Test 4" << std::endl;
        auto systemConfig = SystemConfig();
        systemConfig.bufferPoolSize = 1024 * 1024 * 50; // 50 MB
        auto database = std::make_unique<Database>("tmp");
        std::cout << "Database created." << std::endl;
        auto connection = std::make_unique<Connection>(database.get());

        // Create schema.
        connection->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));");
        // Create nodes.
        connection->query("CREATE (:Person {name: 'Alice', age: 25});");
        connection->query("CREATE (:Person {name: 'Bob', age: 30});");

        // Execute a simple query.
        auto result = connection->query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;");
        // Print query result.
        std::cout << result->toString();
    } catch (const std::exception& e) {
        std::cout << e.what() << std::endl;
    }
}
