cmake_minimum_required(VERSION 3.24 FATAL_ERROR)

project(my_boruvka LANGUAGES CXX CUDA)

set(ESSENTIALS_BUILD_EXAMPLES OFF CACHE BOOL "Disable Gunrock examples" FORCE)
set(ESSENTIALS_BUILD_TESTS OFF CACHE BOOL "Disable Gunrock unit tests" FORCE)
set(ESSENTIALS_BUILD_BENCHMARKS OFF CACHE BOOL "Disable Gunrock benchmarks" FORCE)
set(ESSENTIALS_COLLECT_METRICS OFF CACHE BOOL "Disable Gunrock metrics collection" FORCE)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/gunrock/externals/cmake_modules-src")

enable_language(C)

add_subdirectory(gunrock)

add_executable(boruvka
        boruvka.cu
)

set_target_properties(boruvka PROPERTIES
        CUDA_SEPARABLE_COMPILATION ON
        CUDA_ARCHITECTURES ${CMAKE_CUDA_ARCHITECTURES}
)

target_link_libraries(boruvka PRIVATE essentials)

target_include_directories(boruvka PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

set_target_properties(boruvka PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)
