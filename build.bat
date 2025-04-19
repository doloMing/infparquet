@echo off
setlocal enabledelayedexpansion

echo InfParquet Build Script for Windows
echo ===================================

REM Check if CMake is installed
where cmake >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo Error: CMake is not installed or not in PATH.
    echo Please install CMake from https://cmake.org/download/
    exit /b 1
)

REM Check if Visual Studio is installed by checking for cl.exe
where cl >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo Warning: Microsoft Visual C++ Compiler (cl.exe) not found in PATH.
    echo Build may fail if Visual Studio environment is not properly set up.
)

REM Check if build directory exists, create if not
if not exist build (
    echo Creating build directory...
    mkdir build
)

REM Change to build directory
cd build

REM Command-line arguments
set BUILD_TYPE=Release
set GENERATOR="Visual Studio 17 2022"
set ARROW_PATH=""
set LZMA_PATH=""
set XXHASH_PATH=""

REM Parse command-line arguments
:parse_args
if "%~1" == "" goto :end_parse_args
if /i "%~1" == "-debug" (
    set BUILD_TYPE=Debug
    goto :next_arg
)
if /i "%~1" == "-release" (
    set BUILD_TYPE=Release
    goto :next_arg
)
if /i "%~1" == "-generator" (
    set GENERATOR=%2
    shift
    goto :next_arg
)
if /i "%~1" == "-arrow" (
    set ARROW_PATH=%2
    shift
    goto :next_arg
)
if /i "%~1" == "-lzma" (
    set LZMA_PATH=%2
    shift
    goto :next_arg
)
if /i "%~1" == "-xxhash" (
    set XXHASH_PATH=%2
    shift
    goto :next_arg
)
if /i "%~1" == "-help" (
    echo.
    echo Usage: build.bat [options]
    echo.
    echo Options:
    echo   -debug               Build debug version
    echo   -release             Build release version (default)
    echo   -generator "GEN"     Specify CMake generator
    echo   -arrow PATH          Path to Arrow library
    echo   -lzma PATH           Path to LZMA library
    echo   -xxhash PATH         Path to xxHash library
    echo   -help                Display this help message
    echo.
    exit /b 0
)

echo Unknown option: %1
goto :next_arg

:next_arg
shift
goto :parse_args

:end_parse_args

echo Build type: %BUILD_TYPE%
echo CMake generator: %GENERATOR%

REM Prepare CMake arguments
set CMAKE_ARGS=-DCMAKE_BUILD_TYPE=%BUILD_TYPE%

if not "%ARROW_PATH%" == "" (
    set CMAKE_ARGS=!CMAKE_ARGS! -DARROW_ROOT="%ARROW_PATH%"
)

if not "%LZMA_PATH%" == "" (
    set CMAKE_ARGS=!CMAKE_ARGS! -DLZMA_ROOT="%LZMA_PATH%"
)

if not "%XXHASH_PATH%" == "" (
    set CMAKE_ARGS=!CMAKE_ARGS! -DXXHASH_ROOT="%XXHASH_PATH%"
)

REM Configure with CMake
echo.
echo Configuring with CMake...
cmake -G %GENERATOR% %CMAKE_ARGS% ..

if %ERRORLEVEL% neq 0 (
    echo.
    echo Error: CMake configuration failed.
    exit /b %ERRORLEVEL%
)

REM Build the project
echo.
echo Building InfParquet...
cmake --build . --config %BUILD_TYPE%

if %ERRORLEVEL% neq 0 (
    echo.
    echo Error: Build failed.
    exit /b %ERRORLEVEL%
)

echo.
echo Build completed successfully!
echo The executable is located at: .\bin\%BUILD_TYPE%\infparquet.exe

endlocal 