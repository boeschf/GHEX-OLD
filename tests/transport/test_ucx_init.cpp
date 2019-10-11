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
#include <ghex/transport_layer/callback_communicator.hpp>
#include <ghex/transport_layer/mpi/setup.hpp>
#include <ghex/transport_layer/ucx/environment.hpp>
#include <iostream>
#include <iomanip>
#include <gtest/gtest.h>

TEST(pmix, test0) 
{
    bool ok = true;
    EXPECT_TRUE(ok);

    gridtools::ghex::tl::mpi::setup_communicator comm(MPI_COMM_WORLD);

    gridtools::ghex::tl::ucx::communicator_base ucx_comm(comm);
    
    ucx_comm.test_send_recv();
}

