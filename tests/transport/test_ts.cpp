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
#include <ghex/transport_layer/message_buffer.hpp>
#include <ghex/transport_layer/mpi/communicator.hpp>
#include <ghex/transport_layer/ucx3/address_db_mpi.hpp>
#include <ghex/transport_layer/ucx3/context.hpp>
#include <vector>
#include <iomanip>
#include <atomic>
#include <thread>

#include <gtest/gtest.h>

#define GHEX_TEST_TS_UCX

#ifdef GHEX_TEST_TS_UCX
using db_t            = gridtools::ghex::tl::ucx::address_db_mpi;
using context_t       = gridtools::ghex::tl::context<gridtools::ghex::tl::ucx_tag>;
using comm_t          = typename context_t::communicator_type;
#else
using comm_t          = gridtools::ghex::tl::communicator<gridtools::ghex::tl::mpi_tag>;
#endif

using cont_comm_t     = gridtools::ghex::tl::continuation_communicator;
using msg_type        = gridtools::ghex::tl::message_buffer<>;


// ring-communication using arbitrary number of threads for communication and progressing
// each rank has num_comm_threads threads
// each rank has num_progress_threads which progress the communication and execute the callbacks
//
// num_comm_threads send to the right rank 
// num_comm_threads receive from the left rank 
//
// the messages are passed as l-value references (GHEX does not take ownership)
// there is one exception to show-case the usage of moving in the message, but this does not alter the semantics of
// this test
//
// there are two modes:
// - wait mode:   + each thread waits until the send and receive are finished
//                + this is done using the requests returned by the communicator
//
// - nowait mode: + send and receives are posted, the function returns immediately
//
void test_ring(std::size_t num_progress_threads, std::size_t num_comm_threads, bool wait)
{
    std::atomic<std::size_t> num_completed;
    num_completed.store(0u);

#ifdef GHEX_TEST_TS_UCX
    context_t context{ db_t{MPI_COMM_WORLD} };
    comm_t comm = context.make_communicator();
#else
    comm_t comm;
#endif

    // use basic communicator to establish neighbors
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
#ifdef GHEX_TEST_TS_UCX
        comms.push_back(context.make_communicator());
#else
        comms.push_back(comm_t());
#endif
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
            [](cont_comm_t::message_type m, int r, int t) {
                std::cout << "received from " << r << " with tag " << t << " and size " << m.size() << std::endl; });

        // give up ownership of some message
        // this is just to illustrate the functionality and syntax
        msg_type another_msg(4096);
        another_msg.data<int>()[0] = send_msg.data<int>()[0];
        another_msg.data<int>()[1] = send_msg.data<int>()[1];
        cont_comm.send(c, std::move(another_msg),r_rank,tag,
            [](cont_comm_t::message_type m, int r, int t) {
                std::cout << "sent to       " << r << " with tag " << t << " and size " << m.size() << std::endl; });
    };
    
    // lambda which places send and receive calls and waits for completion
    auto send_recv_func_wait =
    [&cont_comm,l_rank,r_rank](comm_t& c, int tag, msg_type& recv_msg, msg_type& send_msg)
    {
        auto recv_req = cont_comm.recv(c, recv_msg,l_rank,tag,
            [](cont_comm_t::message_type m, int r, int t) {
                std::cout << "received from " << r << " with tag " << t << " and size " << m.size() << std::endl; });

        auto send_req = cont_comm.send(c, send_msg,r_rank,tag,
            [](cont_comm_t::message_type m, int r, int t) {
                std::cout << "sent to       " << r << " with tag " << t << " and size " << m.size() << std::endl; });
        while ( !(recv_req.ready() && send_req.ready()) ) {}
    };

    // lambda which progresses the queues
    auto progress_func =
    [&cont_comm, num_requests, &num_completed]()
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

    std::cout << "end of func" << std::endl;

    //comm.barrier();
}


// send multiple messages from rank 0 (broadcast)
// and repost the same message after it's been sent
// each rank has num_comm_threads threads
// each rank has num_progress_threads which progress the communication and execute the callbacks
//
// rank 0:      num_comm_threads send twice to each rank (using a repost)
// other ranks: num_comm_threads receive twice from rank 0
//
// there are two modes:
// - wait mode:   + each thread waits until the first round of communication has finished and then reposts
//                + this is done using the requests returned by the communicator
//                + the messages are passed as l-value references (GHEX does not take ownership)
//
// - nowait mode: + rank 0:      each thread submits a send_multi. Another thread from the progress thread-pool
//                               executes the callback and resubmits a send_multi after the message has been sent
//                               to all other ranks.
//                + other ranks: two receives are posted (with two different recv messages), the function returns immediately
//                + the messages are passed as r-value references (GHEX takes ownership)
//
void test_send_multi(std::size_t num_progress_threads, std::size_t num_comm_threads, bool wait)
{
    std::atomic<std::size_t> num_completed;
    num_completed.store(0u);

#ifdef GHEX_TEST_TS_UCX
    context_t context{ db_t{MPI_COMM_WORLD} };
    comm_t comm = context.make_communicator();
#else
    comm_t comm;
#endif

    // shared callback communicator
    cont_comm_t cont_comm;

    // per-thread objects
    std::vector<comm_t> comms;
    std::vector<int> num_reps; // used for counting in the first nowait send callback
    for (std::size_t i=0; i<num_comm_threads; ++i)
    {
#ifdef GHEX_TEST_TS_UCX
        comms.push_back(context.make_communicator());
#else
        comms.push_back(comm_t());
#endif
        num_reps.push_back(0);
    }
    
    // total number of send/recv operations per rank
    std::size_t num_requests;
    if (comm.rank() == 0)
        num_requests = 2*num_comm_threads*(comm.size()-1);
    else
        num_requests = 2*num_comm_threads;

    // progress function is equal for all ranks
    auto progress_func =
    [&cont_comm, num_requests, &num_completed]()
    {
        while(num_completed < num_requests)
            num_completed += cont_comm.progress();
    };

    if (comm.rank() == 0)
    {
        // producer rank

        // define neighbors
        std::vector<int> neighbor_ranks;
        for (int i=1; i<comm.size(); ++i)
            neighbor_ranks.push_back(i);
        
        // wait mode
        auto send_multi_wait =
        [&cont_comm,&neighbor_ranks,num_comm_threads](comm_t& c, int tag)
        {
            msg_type msg(4096);
            msg.data<int>()[0] = tag;
            // get a vector of requests
            auto reqs = cont_comm.send_multi(c, msg, neighbor_ranks, tag,
                [](cont_comm_t::message_type m, int r, int t) 
                {
                    std::cout << "sent to       " << r << " with tag " << t << " and size " << m.size() << std::endl; 
                });
            // wait until all requests are done
            bool finished = false;
            while (!finished)
            {
                bool f = true;
                for (auto& r : reqs)
                    f = f && r.ready();
                finished = f;
            }
            std::cout << "reposting" << std::endl;
            reqs = cont_comm.send_multi(c, msg, neighbor_ranks, tag+num_comm_threads,
                [](cont_comm_t::message_type m, int r, int t) 
                {
                    std::cout << "sent to       " << r << " with tag " << t << " and size " << m.size() << std::endl; 
                });
            // wait until all requests are done
            // this is important since otherwise the message will go out of scope and is destroyed
            // and that would lead to corruption since we passed the message as l-value reference
            finished = false;
            while (!finished)
            {
                bool f = true;
                for (auto& r : reqs)
                    f = f && r.ready();
                finished = f;
            }
        };

        // nowait mode
        auto send_multi_nowait =
        [&cont_comm,&neighbor_ranks,num_comm_threads](comm_t& c, int tag, int& num_reps_i)
        {
            const int s = neighbor_ranks.size();
            msg_type msg(4096);
            msg.data<int>()[0] = tag;
            // no return value is required, message is moved in
            cont_comm.send_multi(c, std::move(msg), neighbor_ranks, tag,
                [&num_reps_i,s,&c,&cont_comm,neighbor_ranks,num_comm_threads](cont_comm_t::message_type m, int r, int t) 
                {
                    std::cout << "sent to       " << r << " with tag " << t << " and size " << m.size() << std::endl; 
                    ++num_reps_i;
                    // check if the message has been sent to all ranks
                    if (num_reps_i%s == 0)
                    {
                        std::cout << "reposting" << std::endl;
                        // note the move in the repost:
                        // it recommended to always move inside a callback since this is safe in all cases!
                        // here it is actually required to move and not doing so will lead to bad bad things.
                        cont_comm.send_multi(c, std::move(m), neighbor_ranks, t+num_comm_threads);
                    }
                });
        };
    
        // make threads
        std::vector<std::thread> threads;
        threads.reserve(num_progress_threads+num_comm_threads);

        for (std::size_t i=0; i<num_progress_threads; ++i)
            threads.push_back( std::thread(progress_func) );

        if (wait) 
            for (std::size_t i=0; i<num_comm_threads; ++i)
                threads.push_back( std::thread(
                    send_multi_wait, 
                    std::ref(comms[i]),
                    (int)i) );
        else
            for (std::size_t i=0; i<num_comm_threads; ++i)
                threads.push_back( std::thread(
                    send_multi_nowait,
                    std::ref(comms[i]),
                    (int)i,
                    std::ref(num_reps[i])) );
        
        // wait for completion
        for (auto& t : threads)
            t.join();
    }
    else
    {
        // consumer rank
        
        // wait mode
        auto recv_wait =
        [&cont_comm,num_comm_threads](comm_t& c, int tag)
        {
            msg_type msg(4096);
            msg.data<int>()[0] = -1;
            // get a request as return value
            auto req = cont_comm.recv(c, msg, 0, tag,
                [](cont_comm_t::message_type m, int, int t) 
                {
                    EXPECT_TRUE(reinterpret_cast<int*>(m.data())[0] == t);
                });
            // wait on the request
            while (!req.ready()){}
            msg.data<int>()[0] = -1;
            req = cont_comm.recv(c, msg, 0, tag+num_comm_threads,
                [num_comm_threads](cont_comm_t::message_type m, int, int t) 
                {
                    EXPECT_TRUE(reinterpret_cast<int*>(m.data())[0] == (int)(t-num_comm_threads));
                });
            // wait until the requests is ready
            // this is important since otherwise the message will go out of scope and is destroyed
            // and that would lead to corruption since we passed the message as l-value reference
            while (!req.ready()){}
        };

        // nowait mode
        auto recv_nowait =
        [&cont_comm,num_comm_threads](comm_t& c, int tag)
        {
            msg_type msg(4096);
            msg.data<int>()[0] = -1;
            // no return value is required, message is moved in
            cont_comm.recv(c, std::move(msg), 0, tag,
                [](cont_comm_t::message_type m, int, int t) 
                {
                    EXPECT_TRUE(reinterpret_cast<int*>(m.data())[0] == t);
                });
            // another message is created
            msg_type msg2(4096);
            msg2.data<int>()[0] = -1;
            // no return value is required, message is moved in
            cont_comm.recv(c, std::move(msg2), 0, tag+num_comm_threads,
                [num_comm_threads](cont_comm_t::message_type m, int, int t) 
                {
                    EXPECT_TRUE(reinterpret_cast<int*>(m.data())[0] == (int)(t-num_comm_threads));
                });
        };
    
        // make threads
        std::vector<std::thread> threads;
        threads.reserve(num_progress_threads+num_comm_threads);

        for (std::size_t i=0; i<num_progress_threads; ++i)
            threads.push_back( std::thread(progress_func) );

        if (wait) 
            for (std::size_t i=0; i<num_comm_threads; ++i)
                threads.push_back( std::thread(
                    recv_wait, 
                    std::ref(comms[i]),
                    (int)i) );
        else
            for (std::size_t i=0; i<num_comm_threads; ++i)
                threads.push_back( std::thread(
                    recv_nowait,
                    std::ref(comms[i]),
                    (int)i) );
        
        // wait for completion
        for (auto& t : threads)
            t.join();

    }

    //comm.barrier();
}


TEST(transport, ring) {
    test_ring(2, 3, true);
    test_ring(2, 3, false);
}

TEST(transport, send_multi) {
    test_send_multi(5, 7, true);
    test_send_multi(5, 7, false);
}
