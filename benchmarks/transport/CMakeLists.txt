
set(_benchmarks_mt ghex_p2p_bi_ft_wait_mt_ucx3  ghex_p2p_bi_ft_avail_mt_ucx3 ghex_p2p_bi_cb_avail_mt_ucx3)

foreach(t_ ${_benchmarks_mt})

    add_executable( ${t_} ./${t_}.cpp )
    target_include_directories(${t_} PRIVATE ${CMAKE_SOURCE_DIR}/include ${GTEST_INCLUDE_DIRS} ${UCP_INCLUDE_DIR})
    target_link_libraries(${t_}  GridTools::gridtools MPI::MPI_CXX gtest_main_ucx ${UCP_LIBRARY})
    target_compile_definitions(${t_} PRIVATE -DUSE_OPENMP)

endforeach()
