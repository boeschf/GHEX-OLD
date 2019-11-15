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
#include <omp.h>
#include <cstdlib>

#include <ghex/common/timer.hpp>
//#include <gtest/gtest.h>

namespace ghex = gridtools::ghex;

using db_type      = ghex::tl::ucx::endpoint_db_mpi;
using mpi_context  = ghex::tl::ucx::endpoint_db_mpi_simple;
using context_type = ghex::tl::context<gridtools::ghex::tl::ucx_tag>;
using comm_type    = ghex::tl::ucx::communicator_base;
using message_type = ghex::tl::message_buffer<>;

int main(int argc, char **argv)
{
    int init_result = MPI_Init(&argc, &argv);
    if (init_result == MPI_ERR_OTHER)
        throw std::runtime_error("MPI init failed");

    const auto niter     = std::atoi(argv[1]);
    const auto buff_size = std::atoi(argv[2]);
    const auto inflight  = std::atoi(argv[3]);
    
    int num_threads;
        
    #pragma omp parallel
    {
        #pragma omp master
        {
            num_threads = omp_get_num_threads();
        }
    }
    
    context_type context{ db_type{MPI_COMM_WORLD} };
    
    std::vector<comm_type> comms;
    for (int n=0; n<num_threads+1; ++n)
        comms.push_back( context.make_communicator() );

    if (context.rank() == 0)
        std::cout << "number of threads = " << num_threads << std::endl;

    const int peer_rank = (context.rank()+1)%context.size();

    std::vector<ghex::tl::ucx::endpoint> endpoints;
    for (int n=0; n<num_threads; ++n)
    {
        endpoints.push_back(comms[n+1].connect(peer_rank,0) );
    }
    
    auto send_func = [&](int th_id) {
        // make some message buffers
        ghex::tl::message_buffer<> send_msg(buff_size);
        send_msg.data<char>()[0] = 0;
        using future_type = std::remove_reference_t<decltype(comms[th_id+1].send(send_msg, endpoints[th_id]))>;
        std::vector<future_type> futures(inflight);
        for (int n=0; n<niter; ++n)
        {
            
        }

        //comms[th_id].send(send_msg, endpoints[th_id-1]).wait();
    };

    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Finalize();

    return 0;
}


//TEST(transport_layer, all_to_1)
//{
//    auto nthr = omp_get_num_threads();
//    int num_threads = 4;
//    const std::size_t size = 1<<18;
//
//    // make a ucx context which has access to a connection database
//    // here the database is local to all ranks -> big,
//    // but one can also make a database using pmi for example
//    context_type context{ db_type{MPI_COMM_WORLD} };
//
//    // create as many communicators as there are threads
//    std::vector<comm_type> comms;
//    for (int n=0; n<num_threads; ++n)
//        comms.push_back( context.make_communicator() );
//
//    // synchronize the context -> database knows now about all communicators
//    context.synchronize();
//
//    int rank       = context.rank();
//    int left_rank  = (context.rank()+context.size()-1)%context.size();
//    int right_rank = (context.rank()+context.size()+1)%context.size();
//
//    // connect to left and right endpoint for each communicator
//    std::vector<ghex::tl::ucx::endpoint> endpoints;
//    for (int n=1; n<num_threads; ++n)
//    {
//        endpoints.push_back(comms[n].connect(left_rank,0) );
//    }
//    
//    std::vector<int> check_rank;
//    std::vector<int> check_thread;
//
//    auto send_func = [&](int th_id) {
//        // make some message buffers
//        ghex::tl::message_buffer<> send_msg(size);
//
//        // fill values for checking
//        send_msg.data<int>()[0]  = rank;
//        send_msg.data<int>()[1]  = th_id;
//
//        comms[th_id].send(send_msg, endpoints[th_id-1]).wait();
//    };
//
//
//    auto recv_func = [&](int th_id) {
//        // make some message buffers
//        ghex::tl::message_buffer<> recv_msg(size);
//
//        for (int n=1; n<num_threads; ++n)
//        {
//            comms[th_id].recv(recv_msg).wait();
//            check_rank.push_back(recv_msg.data<int>()[0]);
//            check_thread.push_back(recv_msg.data<int>()[1]);
//        }
//    };
//
//    
//    // run the exchange in seperate threads
//    std::vector<std::thread> threads;
//    threads.push_back( std::thread{recv_func, 0});
//    for(int n=1; n<num_threads; ++n)
//        threads.push_back( std::thread{send_func, n});
//    // wait for the exchange to finish
//    for (auto& t : threads)
//        t.join();
//
//    std::sort(check_thread.begin(), check_thread.end());
//    for (int n=1; n<num_threads; ++n)
//    {
//        EXPECT_TRUE( check_rank[n-1] == right_rank );
//        EXPECT_TRUE( check_thread[n-1] == n );
//    }
//}
