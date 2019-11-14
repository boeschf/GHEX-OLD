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
#include <ghex/transport_layer/continuation_communicator.hpp>
#include <ghex/transport_layer/mpi/communicator.hpp>
#include <vector>
#include <iomanip>
#include <atomic>
#include <thread>

#include <gtest/gtest.h>

using comm_t          = gridtools::ghex::tl::communicator<gridtools::ghex::tl::mpi_tag>;
using cont_comm_t     = gridtools::ghex::tl::continuation_communicator;
using msg_type        = gridtools::ghex::tl::message_buffer<>;

std::atomic<std::size_t> num_completed;

// ring-communication using arbitrary number of threads for communication and progressing
void test1(std::size_t num_progress_threads, std::size_t num_comm_threads, bool wait)
{
    num_completed.store(0u);

    // use basic communicator to establish neighbors
    comm_t comm;
    const int rank   = comm.rank();
    const int r_rank = (rank+1)%comm.size();
    const int l_rank = (rank+comm.size()-1)%comm.size();

    // shared callback communicator
    cont_comm_t cont_comm;

    // per-thread objects
    std::vector<msg_type> send_msgs;
    std::vector<msg_type> recv_msgs;
    std::vector<comm_t>   comms;
    for (std::size_t i=0; i<num_comm_threads; ++i)
    {
        send_msgs.push_back(msg_type(4096));
        recv_msgs.push_back(msg_type(4096));
        comms.push_back(comm_t());
        send_msgs.back().data<int>()[0] = rank;
        send_msgs.back().data<int>()[1] = i;
        recv_msgs.back().data<int>()[0] = -1;
        recv_msgs.back().data<int>()[1] = -1;
    }

    // total number of sends and receives
    std::size_t num_requests = 2*num_comm_threads;

    // lambda which places send and receive calls
    auto send_recv_func_nowait =
    [&cont_comm,l_rank,r_rank](comm_t& c, int tag, msg_type& recv_msg, msg_type& send_msg)
    {
        cont_comm.recv(c, recv_msg,l_rank,tag,
            [](cont_comm_t::message_type, int r, int t) {
                std::cout << "received from " << r << " with tag " << t << std::endl; });

        cont_comm.send(c, send_msg,r_rank,tag,
            [](cont_comm_t::message_type, int r, int t) {
                std::cout << "sent to       " << r << " with tag " << t << std::endl; });
    };
    
    // lambda which places send and receive calls and waits for completion
    auto send_recv_func_wait =
    [&cont_comm,l_rank,r_rank](comm_t& c, int tag, msg_type& recv_msg, msg_type& send_msg)
    {
        auto recv_req = cont_comm.recv(c, recv_msg,l_rank,tag,
            [](cont_comm_t::message_type, int r, int t) {
                std::cout << "received from " << r << " with tag " << t << std::endl; });

        auto send_req = cont_comm.send(c, send_msg,r_rank,tag,
            [](cont_comm_t::message_type, int r, int t) {
                std::cout << "sent to       " << r << " with tag " << t << std::endl; });
        while ( !(recv_req.is_ready() && send_req.is_ready()) ) {}
    };

    // lambda which progresses the queues
    auto progress_func =
    [&cont_comm, num_requests]()
    {
        while(num_completed < num_requests)
            num_completed += cont_comm.progress();
    };

    // make threads
    std::vector<std::thread> threads;
    threads.reserve(num_progress_threads+num_comm_threads);

    for (std::size_t i=0; i<num_progress_threads; ++i)
        threads.push_back( std::thread(progress_func) );

    if (wait) 
        for (std::size_t i=0; i<num_comm_threads; ++i)
            threads.push_back( std::thread(
                send_recv_func_wait, 
                std::ref(comms[i]),
                (int)i,
                std::ref(recv_msgs[i]),
                std::ref(send_msgs[i])) );
    else
        for (std::size_t i=0; i<num_comm_threads; ++i)
            threads.push_back( std::thread(
                send_recv_func_nowait,
                std::ref(comms[i]),
                (int)i,
                std::ref(recv_msgs[i]),
                std::ref(send_msgs[i])) );

    // wait for completion
    for (auto& t : threads)
        t.join();
    
    for (std::size_t i=0; i<num_comm_threads; ++i)
    {
        EXPECT_TRUE(recv_msgs[i].data<int>()[0] == l_rank);
        EXPECT_TRUE(recv_msgs[i].data<int>()[1] == (int)i);
    }

    comm.barrier();
}


TEST(transport, basic) {
    test1(2, 3, true);
    test1(2, 3, false);
}
