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
#include <ghex/transport_layer/ucx2/endpoint_db_mpi_simple.hpp>
#include <ghex/transport_layer/mpi/setup.hpp>
#include <ghex/transport_layer/ucx2/context.hpp>
#include <ghex/transport_layer/message_buffer.hpp>
#include <iostream>
#include <iomanip>
#include <thread>
#include <future>
#include <gtest/gtest.h>

namespace ghex = gridtools::ghex;

using db_type       = ghex::tl::ucx::endpoint_db_mpi;
using mpi_context  = ghex::tl::ucx::endpoint_db_mpi_simple;
using context_type = ghex::tl::context<gridtools::ghex::tl::ucx_tag>;
using comm_type    = ghex::tl::ucx::communicator_base;


TEST(transport_layer, symmetric_bidirectional_ring)
{
    int num_threads = 4;
    const std::size_t size = 1<<18;

    // make a ucx context whithout a connection database - rank and size information is enough
    // since we're using explicit MPI calls for out-of-band connection establishment
    context_type context{ mpi_context{MPI_COMM_WORLD} };
    
    int mpi_rank      = context.rank();
    int num_mpi_ranks = context.size();

    // create as many communicators as there are threads
    std::vector<comm_type> comms;
    for (int n=0; n<num_threads; ++n)
        comms.push_back( context.make_communicator() );

    // no synchronization needed since we're using explicit MPI calls for out-of-band connection establishment
    //context.synchronize();

    // get all endpoints in a serialized format
    std::vector<ghex::tl::ucx::endpoint_info> endpoints;
    for (int n=0; n<num_threads; ++n)
        endpoints.push_back( comms[n].packed_endpoint() );

    // set the neighbors
    std::vector<ghex::tl::ucx::endpoint_info> left_neighbors(endpoints);
    std::vector<ghex::tl::ucx::endpoint_info> right_neighbors(endpoints);
    for (unsigned int i = 0; i < comms.size()-1; i++) {
        left_neighbors[i+1] = endpoints[i];
        right_neighbors[i]  = endpoints[i+1];
    }

    // add the missing neighbors through MPI communication
    MPI_Request req1, req2;
    MPI_Status status1, status2;

    MPI_Irecv(left_neighbors[0].data(), left_neighbors[0].size(), MPI_BYTE, (mpi_rank+num_mpi_ranks-1)%num_mpi_ranks, 1, MPI_COMM_WORLD, &req1);
    MPI_Irecv(right_neighbors[right_neighbors.size()-1].data(), right_neighbors[right_neighbors.size()-1].size(), MPI_BYTE, (mpi_rank+1)%num_mpi_ranks, 2, MPI_COMM_WORLD, &req2);

    MPI_Send(endpoints[endpoints.size()-1].data(), endpoints[endpoints.size()-1].size(), MPI_BYTE, (mpi_rank+1)%num_mpi_ranks, 1, MPI_COMM_WORLD);
    MPI_Send(endpoints[0].data(), endpoints[0].size(), MPI_BYTE, (mpi_rank+num_mpi_ranks-1)%num_mpi_ranks, 2, MPI_COMM_WORLD);

    MPI_Wait(&req1, &status1);
    MPI_Wait(&req2, &status2);

    // create actual endpoints by connecting each communicator to its neighbors
    std::vector<ghex::tl::ucx::endpoint> left_eps;
    std::vector<ghex::tl::ucx::endpoint> right_eps;
    for (int n=0; n<num_threads; ++n)
    {
        left_eps.push_back(  comms[n].connect(left_neighbors[n]) );
        right_eps.push_back( comms[n].connect(right_neighbors[n]) );
    }

    auto func = [&](int th_id) {
        // make some message buffers
        ghex::tl::message_buffer<> sendleft_msg(size);
        ghex::tl::message_buffer<> recvright_msg(size);
        ghex::tl::message_buffer<> sendright_msg(size);
        ghex::tl::message_buffer<> recvleft_msg(size);

        // fill values for checking
        sendleft_msg.data<int>()[0]  = mpi_rank;
        sendleft_msg.data<int>()[1]  = th_id;
        sendright_msg.data<int>()[0] = mpi_rank;
        sendright_msg.data<int>()[1] = th_id;

        auto sfutl = comms[th_id].send(sendleft_msg, left_eps[th_id]);
        auto rfutr = comms[th_id].recv(recvright_msg, right_eps[th_id]);
        auto sfutr = comms[th_id].send(sendright_msg, right_eps[th_id]);
        auto rfutl = comms[th_id].recv(recvleft_msg, left_eps[th_id]);

        sfutl.wait();
        rfutl.wait();
        sfutr.wait();
        rfutl.wait();

        // calculate the values that should have arrived
        int left_rank    = (th_id == 0) ? ((mpi_rank+context.size()-1)%context.size()) : mpi_rank; 
        int left_thread  = (th_id == 0) ? (num_threads-1) : th_id-1; 
        int right_rank   = (th_id == num_threads-1) ? ((mpi_rank+1)%context.size()) : mpi_rank; 
        int right_thread = (th_id == num_threads-1) ? (0) : th_id+1; 

        // check
        EXPECT_TRUE( recvleft_msg.data<int>()[0] == left_rank);
        EXPECT_TRUE( recvleft_msg.data<int>()[1] == left_thread);
        EXPECT_TRUE( recvright_msg.data<int>()[0] == right_rank);
        EXPECT_TRUE( recvright_msg.data<int>()[1] == right_thread);
    };

    // run the exchange in seperate threads
    std::vector<std::future<void>> futures;
    futures.reserve(num_threads);
    for(int n=0; n<num_threads; ++n)
        futures.push_back(std::async(std::launch::async, func, n));
    // wait for the exchange to finish
    for (auto& fut : futures) 
        fut.wait();

    // run the exchange in seperate threads
    std::vector<std::thread> threads;
    for(int n=0; n<num_threads; ++n)
        threads.push_back( std::thread{func, n});
    // wait for the exchange to finish
    for (auto& t : threads)
        t.join();
}


TEST(transport_layer, symmetric_bidirectional_ring_self_discover)
{
    int num_threads = 4;
    const std::size_t size = 1<<18;

    // make a ucx context which has access to a connection database
    // here the database is local to all ranks -> big,
    // but one can also make a database using pmi for example
    context_type context{ db_type{MPI_COMM_WORLD} };

    // create as many communicators as there are threads
    std::vector<comm_type> comms;
    for (int n=0; n<num_threads; ++n)
        comms.push_back( context.make_communicator() );

    // synchronize the context -> database knows now about all communicators
    context.synchronize();
    
    // calculate the neighbor endpoint coordinates (rank, index) for the bidirectional ring
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

    // connect to left and right endpoint for each communicator
    std::vector<ghex::tl::ucx::endpoint> left_endpoints;
    std::vector<ghex::tl::ucx::endpoint> right_endpoints;
    for (int n=0; n<num_threads; ++n)
    {
        left_endpoints.push_back(  comms[n].connect(left_neighbors[n].first,  left_neighbors[n].second) );
        right_endpoints.push_back( comms[n].connect(right_neighbors[n].first, right_neighbors[n].second) );
    }
            
    // this function is invoked by every thread
    auto func = [&](int th_id) {
        // make some message buffers
        ghex::tl::message_buffer<> sendleft_msg(size);
        ghex::tl::message_buffer<> recvright_msg(size);
        ghex::tl::message_buffer<> sendright_msg(size);
        ghex::tl::message_buffer<> recvleft_msg(size);

        // fill values for checking
        sendleft_msg.data<int>()[0]  = rank;
        sendleft_msg.data<int>()[1]  = th_id;
        sendright_msg.data<int>()[0] = rank;
        sendright_msg.data<int>()[1] = th_id;

        // send and recieve to/from left and right neighbors
        auto sfutl = comms[th_id].send(sendleft_msg,  left_endpoints[th_id]);
        auto sfutr = comms[th_id].send(sendright_msg, right_endpoints[th_id]);
        auto rfutl = comms[th_id].recv(recvleft_msg,  left_endpoints[th_id]);
        auto rfutr = comms[th_id].recv(recvright_msg, right_endpoints[th_id]);

        // wait for communication to finish
        sfutl.wait();
        rfutl.wait();
        sfutr.wait();
        rfutl.wait();

        // calculate the values that should have arrived
        int left_rank    = (th_id == 0) ? ((rank+context.size()-1)%context.size()) : rank; 
        int left_thread  = (th_id == 0) ? (num_threads-1) : th_id-1; 
        int right_rank   = (th_id == num_threads-1) ? ((rank+1)%context.size()) : rank; 
        int right_thread = (th_id == num_threads-1) ? (0) : th_id+1; 

        // check
        EXPECT_TRUE( recvleft_msg.data<int>()[0] == left_rank);
        EXPECT_TRUE( recvleft_msg.data<int>()[1] == left_thread);
        EXPECT_TRUE( recvright_msg.data<int>()[0] == right_rank);
        EXPECT_TRUE( recvright_msg.data<int>()[1] == right_thread);
    };

    // run the exchange in seperate threads
    std::vector<std::future<void>> futures;
    futures.reserve(num_threads);
    for(int n=0; n<num_threads; ++n)
        futures.push_back(std::async(std::launch::async, func, n));
    // wait for the exchange to finish
    for (auto& fut : futures) 
        fut.wait();

    // run the exchange in seperate threads
    std::vector<std::thread> threads;
    for(int n=0; n<num_threads; ++n)
        threads.push_back( std::thread{func, n});
    // wait for the exchange to finish
    for (auto& t : threads)
        t.join();
}

TEST(transport_layer, all_to_1)
{
    int num_threads = 4;
    const std::size_t size = 1<<18;

    // make a ucx context which has access to a connection database
    // here the database is local to all ranks -> big,
    // but one can also make a database using pmi for example
    context_type context{ db_type{MPI_COMM_WORLD} };

    // create as many communicators as there are threads
    std::vector<comm_type> comms;
    for (int n=0; n<num_threads; ++n)
        comms.push_back( context.make_communicator() );

    // synchronize the context -> database knows now about all communicators
    context.synchronize();

    int rank       = context.rank();
    int left_rank  = (context.rank()+context.size()-1)%context.size();
    int right_rank = (context.rank()+context.size()+1)%context.size();

    // connect to left and right endpoint for each communicator
    std::vector<ghex::tl::ucx::endpoint> endpoints;
    for (int n=1; n<num_threads; ++n)
    {
        endpoints.push_back(comms[n].connect(left_rank,0) );
    }
    
    std::vector<int> check_rank;
    std::vector<int> check_thread;

    auto send_func = [&](int th_id) {
        // make some message buffers
        ghex::tl::message_buffer<> send_msg(size);

        // fill values for checking
        send_msg.data<int>()[0]  = rank;
        send_msg.data<int>()[1]  = th_id;

        comms[th_id].send(send_msg, endpoints[th_id-1]).wait();
    };

    auto recv_func = [&](int th_id) {
        // make some message buffers
        ghex::tl::message_buffer<> recv_msg(size);

        for (int n=1; n<num_threads; ++n)
        {
            comms[th_id].recv(recv_msg).wait();
            check_rank.push_back(recv_msg.data<int>()[0]);
            check_thread.push_back(recv_msg.data<int>()[1]);
        }
    };

    
    // run the exchange in seperate threads
    std::vector<std::thread> threads;
    threads.push_back( std::thread{recv_func, 0});
    for(int n=1; n<num_threads; ++n)
        threads.push_back( std::thread{send_func, n});
    // wait for the exchange to finish
    for (auto& t : threads)
        t.join();

    std::sort(check_thread.begin(), check_thread.end());
    for (int n=1; n<num_threads; ++n)
    {
        EXPECT_TRUE( check_rank[n-1] == right_rank );
        EXPECT_TRUE( check_thread[n-1] == n );
    }
}

