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
#include <ghex/transport_layer/message_buffer.hpp>
#include <iostream>
#include <iomanip>
#include <thread>
#include <gtest/gtest.h>

namespace ghex = gridtools::ghex;

class test_conf : public ::testing::Test {
protected:
    int n_threads;
    int mpi_rank;
    int num_mpi_ranks;

    virtual void SetUp() {
        n_threads = 4;
        MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
        MPI_Comm_size(MPI_COMM_WORLD, &num_mpi_ranks);

    }
};

using db_type      = ghex::tl::ucx::endpoint_db_mpi;
using context_type = ghex::tl::context<gridtools::ghex::tl::ucx_tag>;
using comm_type    = ghex::tl::ucx::communicator_base;

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



TEST(ucx, mauro)
{
    const std::size_t size = 1<<18;
    context_type context{ db_type{MPI_COMM_WORLD} };

    std::vector<comm_type> comms;
    for (int n=0; n<4; ++n)
        comms.push_back( context.make_communicator() );

    context.synchronize();
    
    int rank       = context.rank();
    int left_rank  = (context.rank()+context.size()-1)%context.size();
    int right_rank = (context.rank()+context.size()+1)%context.size();

    std::vector<std::pair<int,int>> left_neighbors;
    std::vector<std::pair<int,int>> right_neighbors;
    left_neighbors.push_back({left_rank, 3});
    for (int n=1; n<4; ++n)
    {
        left_neighbors.push_back({rank,n-1});
        right_neighbors.push_back({rank,n});
    }
    left_neighbors.push_back({right_rank, 0});

    std::vector<ghex::tl::ucx::endpoint> left_endpoints;
    std::vector<ghex::tl::ucx::endpoint> right_endpoints;
    for (int n=0; n<4; ++n)
    {
        left_endpoints.push_back(  comms[n].connect(left_neighbors[n].first,  left_neighbors[n].second) );
        right_endpoints.push_back( comms[n].connect(right_neighbors[n].first, right_neighbors[n].second) );
    }

    std::vector<std::thread> threads;
    int th_id = 0;
    for(int n=0; n<4; ++n)
    {
        threads.push_back(std::thread{[&, th_id]() {
            
            ghex::tl::message_buffer<> sendleft_msg(size);
            ghex::tl::message_buffer<> recvright_msg(size);
            ghex::tl::message_buffer<> sendright_msg(size);
            ghex::tl::message_buffer<> recvleft_msg(size);

            /*fill_msg(sendleft_msg, somevalue);
            fill_msg(sendright_msg, someothervalue);*/


            auto sfutl = comms[th_id].send(sendleft_msg,  left_endpoints[th_id]);
            auto sfutr = comms[th_id].send(sendright_msg, right_endpoints[th_id]);
            auto rfutl = comms[th_id].recv(recvleft_msg,  left_endpoints[th_id]);
            auto rfutr = comms[th_id].recv(recvright_msg, right_endpoints[th_id]);

            /*sfutl.wait();
            rfutl.wait();
            sfutr.wait();
            rfutl.wait();

            EXPECT_TRUE(message_ok(recvright_msg, checkvalue1));
            EXPECT_TRUE(message_ok(recvleft_msg, checkvalue2));*/
        }});
        th_id++;
    }
}
