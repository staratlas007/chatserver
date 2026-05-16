cmake_minimum_required(VERSION 3.0)
project(main)

#配置编译选项
set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} -g)

#配置头文件搜索路径
#include_directories()

#配置库文件搜索路径
#link_directories()

#设置需要编译的源文件列表
set(SRC_LIST test.cpp)

#设置可执行文件最终存储路径
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)

#把.指定路径下的所有源文件名字放到变量名SRC_LIST里面
#aux_source_directory(. SRC_LIST)

#表示可执行文件server由SRC_LIST中的源文件编译而来
add_executable(server ${SRC_LIST})

#需要连接的库文件
target_link_libraries(server muduo_net muduo_base pthread)
