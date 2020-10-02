cmake_minimum_required(VERSION 3.0)

project(RedisAudioscout VERSION 0.1.0 DESCRIPTION "phash audio index")
include(ExternalProject)

set(MODULE_SRCS module.cpp)

set(CMAKE_BUILD_TYPE RelWithDebInfo)

add_library(auscout MODULE ${MODULE_SRCS})
set_target_properties(auscout PROPERTIES PREFIX "")
target_link_options(auscout PRIVATE "LINKER:-shared,-Bsymbolic")

find_package(Boost 1.67 COMPONENTS filesystem program_options system)

if (Boost_FOUND)

  ExternalProject_Add(hiredis
	PREFIX ${CMAKE_CURRENT_BINARY_DIR}/hiredis
	URL https://github.com/redis/hiredis/archive/v1.0.0.tar.gz
	CONFIGURE_COMMAND ""
	BUILD_COMMAND echo make static
	BUILD_IN_SOURCE 1
	INSTALL_COMMAND make install PREFIX=<INSTALL_DIR>)

  set(HIREDIS_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/hiredis/include/hiredis)
  set(HIREDIS_LIB ${CMAKE_CURRENT_BINARY_DIR}/hiredis/lib/libhiredis.a)

  
  find_library(libAudioData AudioData)
  if (libAudioData-NOTFOUND)
	message(WARNING "no libAudioData found")
  endif()
  find_library(libpHashAudio pHashAudio)
  if (libpHashAudio-NOTFOUND)
	message(WARNING "no libpHashAudio found")
  endif()

  add_executable(auscoutclient auscout.cpp)
  add_dependencies(auscoutclient hiredis)
  target_include_directories(auscoutclient PRIVATE ${Boost_INCLUDE_DIR} ${HIREDIS_INCLUDE_DIR})
  target_link_libraries(auscoutclient ${Boost_LIBRARIES} ${HIREDIS_LIB} ${libAudioData} ${libpHashAudio})

  add_executable(testclient testclient.cpp)
  add_dependencies(testclient hiredis)
  target_include_directories(testclient PRIVATE ${HIREDIS_INCLUDE_DIR})
  target_link_libraries(testclient ${HIREDIS_LIB})

endif()

  
install(TARGETS auscout DESTINATION lib PERMISSIONS WORLD_EXECUTE)

