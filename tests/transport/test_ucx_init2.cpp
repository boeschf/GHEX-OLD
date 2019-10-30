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
    context_type ucx_context{ db_type{MPI_COMM_WORLD}  };

    { 
        auto comm0_0 = ucx_context.make_communicator();
        auto comm1_0 = ucx_context.make_communicator();

        ucx_context.synchronize();
    }   

    auto comm0 = ucx_context.make_communicator();
    auto comm1 = ucx_context.make_communicator();

    ucx_context.synchronize();

    if (comm0.rank() == 0)
    {
        std::cout << comm0 << std::endl;
        std::cout << "number of PEs = " << comm0.size() << std::endl;
    }

    int left_neighbor = (comm0.rank()+comm0.size()-1)%comm0.size();
    int right_neighbor = (comm0.rank()+comm0.size()+1)%comm0.size();

    auto ep_0_right_1 = comm0.connect(right_neighbor,1);
    auto ep_1_left_0  = comm1.connect(left_neighbor, 0);

    int msg = 99;

    comm0.send(msg,ep_0_right_1);
    comm1.recv(msg,ep_1_left_0);
}
