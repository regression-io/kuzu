#include <signal.h>

#include "graph_test/graph_test.h"
#include "main/kuzu.h"

using namespace kuzu::common;
using namespace kuzu::testing;

namespace kuzu {
namespace testing {

TEST_F(EmptyDBTest, testReadWrite) {
    systemConfig->accessMode = AccessMode::READ_WRITE;
    auto db = std::make_unique<main::Database>(databasePath, *systemConfig);
    auto con = std::make_unique<main::Connection>(db.get());
    // try create a node table, create nodes, success
    ASSERT_TRUE(con->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))")
                    ->isSuccess());
    ASSERT_TRUE(con->query("CREATE (:Person {name: 'Alice', age: 25})")->isSuccess());
    // try read from node table, success
    ASSERT_TRUE(con->query("MATCH (:Person) RETURN COUNT(*)")->isSuccess());

    db.reset();
    systemConfig->accessMode = AccessMode::READ_ONLY;
    std::unique_ptr<main::Database> db2;
    std::unique_ptr<main::Connection> con2;
    EXPECT_NO_THROW(db2 = std::make_unique<main::Database>(databasePath, *systemConfig));
    EXPECT_NO_THROW(con2 = std::make_unique<main::Connection>(db2.get()));
    // try write, fail
    EXPECT_ANY_THROW(con2->query("DROP TABLE Person"));
    // try read from node table, success
    EXPECT_NO_THROW(con2->query("MATCH (:Person) RETURN COUNT(*)"));
}

} // namespace testing
} // namespace kuzu
