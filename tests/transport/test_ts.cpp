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
#include <ghex/transport_layer/callback_communicator_ts.hpp>
#include <ghex/transport_layer/mpi/communicator.hpp>
#include <vector>
#include <iomanip>
#include <atomic>
#include <thread>

#include <gtest/gtest.h>

using comm_t          = gridtools::ghex::tl::communicator<gridtools::ghex::tl::mpi_tag>;
using callback_comm_t = gridtools::ghex::tl::callback_communicator_ts<comm_t,std::allocator<unsigned char>>;

std::atomic<std::size_t> num_completed;

void test1(std::size_t num_progress_threads, std::size_t num_comm_threads, bool wait)
{
    num_completed.store(0u);

    comm_t comm;
    callback_comm_t cb_comm(comm); 

    const int rank = cb_comm.rank();
    const int r_rank = (rank+1)%cb_comm.size();
    const int l_rank = (rank+cb_comm.size()-1)%cb_comm.size();

    using msg_type = callback_comm_t::message_type;
    using req_type = callback_comm_t::request;

    std::vector<msg_type> send_msgs;
    std::vector<msg_type> recv_msgs;
    std::vector<req_type> send_reqs;
    std::vector<req_type> recv_reqs;
    for (std::size_t i=0; i<num_comm_threads; ++i)
    {
        send_msgs.push_back(msg_type(4096));
        recv_msgs.push_back(msg_type(4096));
        send_reqs.push_back(req_type());
        recv_reqs.push_back(req_type());
    }

    std::size_t num_requests = 2*num_comm_threads;

    // lambda which places send and receive calls
    auto send_recv_func_nowait =
    [&cb_comm,l_rank,r_rank](int tag, msg_type recv_msg, msg_type send_msg)
    {
        cb_comm.recv(recv_msg,l_rank,tag,
            [](callback_comm_t::message_type, int r, int t) {
                std::cout << "received from " << r << " with tag " << t << std::endl; });
        cb_comm.send(send_msg,r_rank,tag,
            [](callback_comm_t::message_type, int r, int t) {
                std::cout << "sent to       " << r << " with tag " << t << std::endl; });
    };
    
    // lambda which places send and receive calls and waits for completion
    auto send_recv_func_wait =
    [&cb_comm,l_rank,r_rank](int tag, msg_type recv_msg, msg_type send_msg, req_type recv_req, req_type send_req)
    {
        recv_req = cb_comm.recv(recv_msg,l_rank,tag,
            [](callback_comm_t::message_type, int r, int t) {
                std::cout << "received from " << r << " with tag " << t << std::endl; });
        send_req = cb_comm.send(send_msg,r_rank,tag,
            [](callback_comm_t::message_type, int r, int t) {
                std::cout << "sent to       " << r << " with tag " << t << std::endl; });
        while ( !(recv_req.is_ready() && send_req.is_ready()) ) {}
    };

    // lambda which progresses the queues
    auto progress_func =
    [&cb_comm, num_requests]()
    {
        while(num_completed < num_requests)
        {
            num_completed += cb_comm.progress();
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(num_progress_threads+num_comm_threads);

    for (std::size_t i=0; i<num_progress_threads; ++i)
        threads.push_back( std::thread(progress_func) );

    if (wait) 
        for (std::size_t i=0; i<num_comm_threads; ++i)
            threads.push_back( std::thread(
                send_recv_func_wait, 
                (int)i,
                recv_msgs[i],
                send_msgs[i],
                recv_reqs[i],
                send_reqs[i]) );
    else
        for (std::size_t i=0; i<num_comm_threads; ++i)
            threads.push_back( std::thread(
                send_recv_func_nowait,
                (int)i,
                recv_msgs[i],
                send_msgs[i]) );

    for (auto& t : threads)
        t.join();

    comm.barrier();
}


TEST(transport, basic) {
    test1(2, 3, true);
    test1(2, 3, false);
}
