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

void test1(std::size_t num_progress_threads)
{
    comm_t comm;
    callback_comm_t cb_comm(comm); 

    const int rank = cb_comm.rank();
    const int r_rank = (rank+1)%cb_comm.size();
    const int l_rank = (rank+cb_comm.size()-1)%cb_comm.size();

    callback_comm_t::message_type send_msg(4096);
    callback_comm_t::message_type recv_msg(4096);
    callback_comm_t::request send_req;
    callback_comm_t::request recv_req;

    std::size_t num_requests = 2;

    auto send_recv_func =
    [&cb_comm,&recv_msg,&send_msg,&recv_req,&send_req,l_rank,r_rank]()
    {
        /*recv_req =*/ cb_comm.recv(recv_msg,l_rank,0,[](callback_comm_t::message_type, int, int) { std::cout << "received!" << std::endl; });
        /*send_req =*/ cb_comm.send(send_msg,r_rank,0,[](callback_comm_t::message_type, int, int) { std::cout << "sent!" << std::endl; });
        //while ( !(recv_req.is_ready() && send_req.is_ready()) ) {}
    };

    auto progress_func =
    [&cb_comm, num_requests]()
    {
        while(num_completed < num_requests)
        {
            num_completed += cb_comm.progress();
        }
    };

    std::vector<std::thread> progress_threads;
    progress_threads.reserve(num_progress_threads+1);

    for (std::size_t i=0; i<num_progress_threads; ++i)
        progress_threads.push_back( std::thread(progress_func) );
        
    progress_threads.push_back( std::thread(send_recv_func) );


    for (auto& t : progress_threads)
        t.join();
}


TEST(transport, basic) {
    test1(2);
}
