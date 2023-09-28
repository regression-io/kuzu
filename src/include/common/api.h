#pragma once

// Generic helper definitions for shared library support
#if defined _WIN32 || defined __CYGWIN__
#define KUZU_HELPER_DLL_IMPORT __declspec(dllimport)
#define KUZU_HELPER_DLL_EXPORT __declspec(dllexport)
#define KUZU_HELPER_DLL_LOCAL
#else
#if __GNUC__ >= 4
#define KUZU_HELPER_DLL_IMPORT __attribute__((visibility("default")))
#define KUZU_HELPER_DLL_EXPORT __attribute__((visibility("default")))
#define KUZU_HELPER_DLL_LOCAL __attribute__((visibility("hidden")))
#else
#define KUZU_HELPER_DLL_IMPORT
#define KUZU_HELPER_DLL_EXPORT
#define KUZU_HELPER_DLL_LOCAL
#endif
#endif

// KUZU_API is used for the public API symbols. It either DLL imports or DLL exports (or does
// nothing for static build) KUZU_LOCAL is used for non-api symbols.

#ifdef KUZU_DLL         // defined if KUZU is compiled as a DLL
#ifdef KUZU_DLL_EXPORTS // defined if we are building the KUZU DLL (instead of using it)
#define KUZU_API KUZU_HELPER_DLL_EXPORT
#else
#define KUZU_API KUZU_HELPER_DLL_IMPORT
#endif // KUZU_DLL_EXPORTS
#define KUZU_LOCAL KUZU_HELPER_DLL_LOCAL
#else // KUZU_DLL is not defined: this means KUZU is a static lib.
#define KUZU_API
#define KUZU_LOCAL
#endif // KUZU_DLL
