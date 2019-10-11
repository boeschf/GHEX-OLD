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
#include <ghex/transport_layer/ucx/communicator.hpp>
#include <iostream>
#include <iomanip>
#include <gtest/gtest.h>

TEST(ucx, send_recv) 
{
    //bool ok = true;
    //EXPECT_TRUE(ok);

    gridtools::ghex::tl::mpi::setup_communicator comm(MPI_COMM_WORLD);

    gridtools::ghex::tl::communicator<gridtools::ghex::tl::ucx_tag> ucx_comm(comm);

    
    //ucx_comm.test_send_recv();
    

    int send_data[3] = { ucx_comm.rank(), ucx_comm.rank(), ucx_comm.rank() };
    int recv_data[3] = { -1, -1, -1 };
                        
    int next_rank = (ucx_comm.rank()+1)%ucx_comm.size();
    int prev_rank = (ucx_comm.rank()+ucx_comm.size()-1)%ucx_comm.size();

    auto fut_send = ucx_comm.send(next_rank, 99, send_data, 3);
    auto fut_recv = ucx_comm.recv(prev_rank, 99, recv_data, 3);

    fut_send.wait();
    fut_recv.wait();

    EXPECT_TRUE( recv_data[0] == prev_rank );
    EXPECT_TRUE( recv_data[1] == prev_rank );
    EXPECT_TRUE( recv_data[2] == prev_rank );

}

