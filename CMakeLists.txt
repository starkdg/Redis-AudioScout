cmake_minimum_required(VERSION 3.0)

project(RedisAudioscout VERSION 0.0.0 DESCRIPTION "phash audio index")
include(ExternalProject)

set(MODULE_SRCS auscout_redis.cpp)
set(CMAKE_INSTALL_PREFIX /var/local)

set(CMAKE_BUILD_TYPE DEBUG)

add_library(auscout MODULE ${MODULE_SRCS})
set_target_properties(auscout PROPERTIES PREFIX "")
target_link_options(auscout PRIVATE "LINKER:-shared,-Bsymbolic")

set(HIREDIS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/hiredis)
set(HIREDIS_INCLUDE_DIRS ${HIREDIS_DIR})
ExternalProject_add(hiredis
  PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/hiredis
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/hiredis
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND echo configure
  BUILD_COMMAND make static
  INSTALL_COMMAND echo install)


find_package(Boost 1.67 COMPONENTS filesystem program_options system)
find_library(libAudioData AudioData)
if (libAudioData-NOTFOUND)
  message(WARNING "no libAudioData found")
endif()
find_library(libpHashAudio pHashAudio)
if (libpHashAudio-NOTFOUND)
  message(WARNING "no libpHashAudio found")
endif()

add_executable(auscoutclient auscout.cpp)
target_include_directories(auscoutclient PRIVATE ${Boost_INCLUDE_DIR})
set_target_properties(auscoutclient PROPERTIES INCLUDE_DIRECTORIES ${HIREDIS_INCLUDE_DIRS})
target_link_libraries(auscoutclient ${Boost_LIBRARIES} ${HIREDIS_DIR}/libhiredis.a  ${libAudioData} ${libpHashAudio})

add_executable(testclient testclient.cpp)
set_target_properties(testclient PROPERTIES INCLUDE_DIRECTORIES ${HIREDIS_INCLUDE_DIRS})
target_link_libraries(testclient ${HIREDIS_DIR}/libhiredis.a)

install(TARGETS auscout DESTINATION lib)

