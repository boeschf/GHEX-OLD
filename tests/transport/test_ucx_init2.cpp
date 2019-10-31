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
#include <future>
#include <gtest/gtest.h>

namespace ghex = gridtools::ghex;

using db_type      = ghex::tl::ucx::endpoint_db_mpi;
using context_type = ghex::tl::context<gridtools::ghex::tl::ucx_tag>;
using comm_type    = ghex::tl::ucx::communicator_base;

TEST(transport_layer, symmetric_bidirectional_ring)
{
    int num_threads = 4;
    const std::size_t size = 1<<18;

    context_type context{ db_type{MPI_COMM_WORLD} };

    std::vector<comm_type> comms;
    for (int n=0; n<num_threads; ++n)
        comms.push_back( context.make_communicator() );

    context.synchronize();
    
    int rank       = context.rank();
    int left_rank  = (context.rank()+context.size()-1)%context.size();
    int right_rank = (context.rank()+context.size()+1)%context.size();

    std::vector<std::pair<int,int>> left_neighbors;
    std::vector<std::pair<int,int>> right_neighbors;
    left_neighbors.push_back(std::make_pair(left_rank, num_threads-1));
    for (int n=1; n<num_threads; ++n)
    {
        left_neighbors.push_back(std::make_pair(rank,n-1));
        right_neighbors.push_back(std::make_pair(rank,n));
    }
    right_neighbors.push_back(std::make_pair(right_rank, 0));

    std::vector<ghex::tl::ucx::endpoint> left_endpoints;
    std::vector<ghex::tl::ucx::endpoint> right_endpoints;
    for (int n=0; n<num_threads; ++n)
    {
        left_endpoints.push_back(  comms[n].connect(left_neighbors[n].first,  left_neighbors[n].second) );
        right_endpoints.push_back( comms[n].connect(right_neighbors[n].first, right_neighbors[n].second) );
    }
            

    auto func = [&](int th_id) {
            
            ghex::tl::message_buffer<> sendleft_msg(size);
            ghex::tl::message_buffer<> recvright_msg(size);
            ghex::tl::message_buffer<> sendright_msg(size);
            ghex::tl::message_buffer<> recvleft_msg(size);

            sendleft_msg.data<int>()[0]  = rank;
            sendleft_msg.data<int>()[1]  = th_id;
            sendright_msg.data<int>()[0] = rank;
            sendright_msg.data<int>()[1] = th_id;

            auto sfutl = comms[th_id].send(sendleft_msg,  left_endpoints[th_id]);
            auto sfutr = comms[th_id].send(sendright_msg, right_endpoints[th_id]);
            auto rfutl = comms[th_id].recv(recvleft_msg,  left_endpoints[th_id]);
            auto rfutr = comms[th_id].recv(recvright_msg, right_endpoints[th_id]);

            sfutl.wait();
            rfutl.wait();
            sfutr.wait();
            rfutl.wait();

            int left_rank    = (th_id == 0) ? ((rank+context.size()-1)%context.size()) : rank; 
            int left_thread  = (th_id == 0) ? (num_threads-1) : th_id-1; 
            int right_rank   = (th_id == num_threads-1) ? ((rank+1)%context.size()) : rank; 
            int right_thread = (th_id == num_threads-1) ? (0) : th_id+1; 

            EXPECT_TRUE( recvleft_msg.data<int>()[0] == left_rank);
            EXPECT_TRUE( recvleft_msg.data<int>()[1] == left_thread);
            EXPECT_TRUE( recvright_msg.data<int>()[0] == right_rank);
            EXPECT_TRUE( recvright_msg.data<int>()[1] == right_thread);
    };

    std::vector<std::future<void>> futures;
    futures.reserve(num_threads);
    for(int n=0; n<num_threads; ++n)
        futures.push_back(std::async(std::launch::async, func, n));

    for(int n=0; n<num_threads; ++n)
        futures[n].wait();
}
