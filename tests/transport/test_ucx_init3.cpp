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

#include <iostream>
#include <ghex/transport_layer/ucx3/address_db_mpi.hpp>
#include <ghex/transport_layer/ucx3/context.hpp>


#include <gtest/gtest.h>

namespace ghex = gridtools::ghex;

using db_type      = ghex::tl::ucx::address_db_mpi;
using context_type = ghex::tl::context<gridtools::ghex::tl::ucx_tag>;

TEST(transport_layer, ucx_context)
{

    context_type context{ db_type{MPI_COMM_WORLD} };

    auto comm = context.make_communicator();
    
    std::cout << "rank " << comm.rank() << std::endl;

    if (comm.rank() == 0)
    {
        std::vector<int> msg(64);
        auto req = comm.recv(msg,1,99);
        req.wait();
        std::cout << msg[0] << std::endl;
    }
    if (comm.rank() == 1)
    {
        std::vector<int> msg(64);
        msg[0] = 42;
        auto req = comm.send(msg,0,99);
        req.wait();
    }

}
