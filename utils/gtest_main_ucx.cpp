/*
 * GridTools
 *
 * Copyright (c) 2019, ETH Zurich
 * All rights reserved.
 *
 * Please, refer to the LICENSE file in the root directory.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#ifndef INCLUDED_GTEST_MAIN_UCX_CPP
#define INCLUDED_GTEST_MAIN_UCX_CPP

#include <fstream>
#include <mpi.h>
#include "gtest/gtest.h"
#include <gridtools/tools/mpi_unit_test_driver/mpi_listener.hpp>
#include <ghex/transport_layer/ucx/environment.hpp>

#ifdef __CUDACC__
#include <cuda_runtime.h>
#include <gridtools/common/cuda_util.hpp>
#endif


GTEST_API_ int main(int argc, char **argv) {

    //gridtools::ghex::tl::pmix::environment env;

    std::cout << "  00000" << std::endl;
    int init_result = MPI_Init(&argc, &argv);
    //int required = MPI_THREAD_MULTIPLE;
    //int provided;
    //int init_result = MPI_Init_thread(&argc, &argv, required, &provided);
    if (init_result == MPI_ERR_OTHER)
        throw std::runtime_error("MPI init failed");
    //if (provided < required)
    //    throw std::runtime_error("MPI does not support required threading level");

    // printf("Running main() from %s\n", __FILE__);
    testing::InitGoogleTest(&argc, argv);

    std::cout << "  11111" << std::endl;
    // set up a custom listener that prints messages in an MPI-friendly way
    auto &listeners = testing::UnitTest::GetInstance()->listeners();
    // first delete the original printer
    delete listeners.Release(listeners.default_result_printer());
    // now add our custom printer
    listeners.Append(new mpi_listener("results_tests"));
    std::cout << "  22222" << std::endl;

    // record the local return value for tests run on this mpi rank
    //      0 : success
    //      1 : failure
    auto result = RUN_ALL_TESTS();

    // perform global collective, to ensure that all ranks return
    // the same exit code
    decltype(result) global_result{};
    MPI_Allreduce(&result, &global_result, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    std::cout << "  33333" << std::endl;

    MPI_Barrier(MPI_COMM_WORLD);
    std::cout << "before MPI finalize" << std::endl;

    MPI_Finalize();

    return global_result;

}

#endif /* INCLUDED_GTEST_MAIN_UCX_CPP */

