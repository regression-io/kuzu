#include <signal.h>

#include "graph_test/graph_test.h"
#include "main/kuzu.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/mman.h>
#include <unistd.h>
#endif

#ifdef __MVS__
#define MAP_ANONYMOUS 0x0
#endif

using namespace kuzu::testing;
using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace testing {

TEST_F(EmptyDBTest, testReadLock) {
#ifdef _WIN32
    HANDLE hFileMapping = CreateFileMappingW(
        INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE, 0, sizeof(uint64_t), L"MySharedMemory");

    uint64_t* count =
        (uint64_t*)MapViewOfFile(hFileMapping, FILE_MAP_ALL_ACCESS, 0, 0, sizeof(uint64_t));
    *count = 0;

#else
    uint64_t* count = (uint64_t*)mmap(
        NULL, sizeof(uint64_t), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, 0, 0);
    *count = 0;
    //create db
    EXPECT_NO_THROW(createDBAndConn());
    ASSERT_TRUE(
        conn->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")
            ->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (:Person {name: 'Alice', age: 25});")->isSuccess());
    database.reset();
    // test read write db
    pid_t pid = fork();
    if (pid == 0) {
        systemConfig->accessMode = AccessMode::READ_ONLY;
        EXPECT_NO_THROW(createDBAndConn());
        (*count)++;
        ASSERT_TRUE(conn->query("MATCH (:Person) RETURN COUNT(*)")->isSuccess());
        while (true) {
            usleep(100);
            ASSERT_TRUE(conn->query("MATCH (:Person) RETURN COUNT(*)")->isSuccess());
        }
    } else if (pid > 0) {
        while (*count == 0) {
            usleep(100);
        }
        systemConfig->accessMode = AccessMode::READ_WRITE;
        // try to open db for writing, this should fail
        EXPECT_ANY_THROW(createDBAndConn());
        // but opening db for reading should work
        systemConfig->accessMode = AccessMode::READ_ONLY;
        EXPECT_NO_THROW(createDBAndConn());
        ASSERT_TRUE(conn->query("MATCH (:Person) RETURN COUNT(*)")->isSuccess());
        // kill the child
        if (kill(pid, SIGKILL) != 0) {
            FAIL();
        }
    }
#endif
}

TEST_F(EmptyDBTest, testWriteLock) {
#ifdef _WIN32
    HANDLE hFileMapping = CreateFileMappingW(
        INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE, 0, sizeof(uint64_t), L"MySharedMemory");

    uint64_t* count =
        (uint64_t*)MapViewOfFile(hFileMapping, FILE_MAP_ALL_ACCESS, 0, 0, sizeof(uint64_t));
    *count = 0;

    // STARTUPINFO si = { sizeof(STARTUPINFO) };
    // PROCESS_INFORMATION pi;
    // if (!CreateProcess(NULL, const_cast<LPSTR>("child.exe"), NULL, NULL, FALSE, 0, NULL, NULL,
    // &si, &pi)) {
    //     std::cerr << "CreateProcess failed: " << GetLastError() << std::endl;
    //     UnmapViewOfFile(count);
    //     CloseHandle(hFileMapping);
    //     return 1;
    // }

    // while (*count == 0) {
    //     Sleep(100);
    // }
    // TerminateProcess(pi.hProcess, 0);
    // CloseHandle(pi.hProcess);
    // CloseHandle(pi.hThread);
    // UnmapViewOfFile(count);
    // CloseHandle(hFileMapping);

#else
    uint64_t* count = (uint64_t*)mmap(
        NULL, sizeof(uint64_t), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, 0, 0);
    *count = 0;
    // test write lock
    // fork away a child
    pid_t pid = fork();
    if (pid == 0) {
        // child process
        // open db for writing
        systemConfig->accessMode = AccessMode::READ_WRITE;
        EXPECT_NO_THROW(createDBAndConn());
        // opened db for writing
        // insert some values
        (*count)++;
        ASSERT_TRUE(
            conn->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")
                ->isSuccess());
        ASSERT_TRUE(conn->query("CREATE (:Person {name: 'Alice', age: 25});")->isSuccess());
        while (true) {
            ASSERT_TRUE(conn->query("MATCH (:Person) RETURN COUNT(*)")->isSuccess());
            usleep(100);
        }
    } else if (pid > 0) {
        // parent process
        // sleep a bit to wait for child process
        while (*count == 0) {
            usleep(100);
        }
        // try to open db for writing, this should fail
        systemConfig->accessMode = AccessMode::READ_WRITE;
        EXPECT_ANY_THROW(createDBAndConn());
        // try to open db for reading, this should fail
        systemConfig->accessMode = AccessMode::READ_ONLY;
        EXPECT_ANY_THROW(createDBAndConn());
        // kill the child
        if (kill(pid, SIGKILL) != 0) {
            FAIL();
        }
    }
#endif
}

TEST_F(EmptyDBTest, testReadOnly) {
#ifdef _WIN32
    HANDLE hFileMapping = CreateFileMappingW(
        INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE, 0, sizeof(uint64_t), L"MySharedMemory");

    uint64_t* count =
        (uint64_t*)MapViewOfFile(hFileMapping, FILE_MAP_ALL_ACCESS, 0, 0, sizeof(uint64_t));
    *count = 0;

#else
    uint64_t* count = (uint64_t*)mmap(
        NULL, sizeof(uint64_t), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, 0, 0);
    *count = 0;
    // cannot create a read-only database in a new directory
    systemConfig->accessMode = AccessMode::READ_ONLY;
    EXPECT_ANY_THROW(createDBAndConn());

    // create the database file and initialize it with data
    pid_t create_pid = fork();
    if (create_pid == 0) {
        systemConfig->accessMode = AccessMode::READ_WRITE;
        EXPECT_NO_THROW(createDBAndConn());
        ASSERT_TRUE(
            conn->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")
                ->isSuccess());
        ASSERT_TRUE(conn->query("CREATE (:Person {name: 'Alice', age: 25});")->isSuccess());
        exit(0);
    }
    waitpid(create_pid, NULL, 0);

    // now connect in read-only mode
    systemConfig->accessMode = AccessMode::READ_ONLY;
    EXPECT_NO_THROW(createDBAndConn());
    // we can query the database
    ASSERT_TRUE(conn->query("MATCH (:Person) RETURN COUNT(*)")->isSuccess());
    // however, we can't perform DDL statements
    EXPECT_ANY_THROW(conn->query("CREATE NODE TABLE university(ID INT64, PRIMARY KEY(ID))"));
    EXPECT_ANY_THROW(conn->query("ALTER TABLE Peron DROP name"));
    EXPECT_ANY_THROW(conn->query("DROP TABLE Peron"));
    // neither can we insert/update/delete data
    EXPECT_ANY_THROW(conn->query("CREATE (:Person {name: 'Bob', age: 25});"));
    EXPECT_ANY_THROW(conn->query("MATCH (p:Person) WHERE p.name='Alice' SET p.age=26;"));
    EXPECT_ANY_THROW(conn->query("MATCH (p:Person) WHERE name='Alice' DELETE p;"));
    // we can run explain queries
    // TODO: BUG
    // EXPECT_NO_THROW(conn->query("EXPLAIN MATCH (p:person) RETURN p.name"));

#endif
}

} // namespace testing
} // namespace kuzu
