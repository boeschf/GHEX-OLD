/* 
 * GridTools
 * 
 * Copyright (c) 2014-2019, ETH Zurich
 * All rights reserved.
 * 
 * Please, refer to the LICENSE file in the root directory.
 * SPDX-License-Identifier: BSD-3-Clause
 * 
 */
//#include <ghex/transport_layer/callback_communicator.hpp>
#include <ghex/transport_layer/ucx2/endpoint_db_mpi.hpp>
#include <ghex/transport_layer/mpi/setup.hpp>
#include <ghex/transport_layer/ucx2/context.hpp>
#include <iostream>
#include <iomanip>
#include <gtest/gtest.h>

using db_type      = gridtools::ghex::tl::ucx::endpoint_db_mpi;
using context_type = gridtools::ghex::tl::context<gridtools::ghex::tl::ucx_tag>;

TEST(ucx, init) 
{
    //bool ok = true;
    //EXPECT_TRUE(ok);

    /*gridtools::ghex::tl::mpi::setup_communicator comm(MPI_COMM_WORLD);

    gridtools::ghex

    gridtools::ghex::tl::context<gridtools::ghex::tl::ucx_tag> ucx_context(comm.size());*/

    context_type ucx_context{ db_type{MPI_COMM_WORLD}  };

    auto comm0 = ucx_context.make_communicator();
    auto comm1 = ucx_context.make_communicator();

    std::cout << comm0 << std::endl;
    std::cout << comm1 << std::endl;

}
