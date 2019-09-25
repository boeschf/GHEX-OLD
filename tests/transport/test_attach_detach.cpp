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

#include <transport_layer/callback_communicator.hpp>
#include <transport_layer/mpi/communicator.hpp>
#include <iostream>
#include <iomanip>
#include <gtest/gtest.h>


using allocator_type     = std::allocator<unsigned char>;
using comm_type          = gridtools::ghex::mpi::communicator;
using callback_comm_type = gridtools::ghex::callback_communicator<comm_type,allocator_type>;
using message_type       = typename callback_comm_type::message_type;

const unsigned int SIZE = 1<<12;

TEST(attach, attach_progress)
{
    bool ok = true;
    comm_type     comm;
    callback_comm_type cb_comm(comm);

    int cb_count = 0;
    message_type send_msg{SIZE,SIZE};
    message_type recv_msg{SIZE,SIZE};

    for (unsigned int i=0; i<SIZE/sizeof(int); ++i)
    {
        send_msg.data<int>()[i] = i*10+comm.rank();
        recv_msg.data<int>()[i] = i*10+comm.rank();
    }

    const auto dst = (comm.rank()+comm.size()+1)%comm.size();
    const auto src = (comm.rank()+comm.size()-1)%comm.size();
    auto send_future = comm.send(send_msg, dst, 0);
    auto recv_future = comm.recv(recv_msg, src, 0);

    cb_comm.attach_send(std::move(send_future), send_msg, dst, 0, [&cb_count](int,int,const message_type&){++cb_count;});
    cb_comm.attach_recv(std::move(recv_future), recv_msg, src, 0, [&cb_count](int,int,const message_type&){++cb_count;});

    ok = ok && (cb_comm.pending_sends()==1);
    ok = ok && (cb_comm.pending_recvs()==1);

    while(cb_comm.progress()){}

    ok = ok && (cb_count == 2);

    for (unsigned int i=0; i<SIZE/sizeof(int); ++i)
        ok = ok && (recv_msg.data<int>()[i] == (int)(i*10+src));

    EXPECT_TRUE(ok);

    MPI_Barrier(comm); 
}

TEST(detach, detach_wait)
{
    bool ok = true;
    comm_type     comm;
    callback_comm_type cb_comm(comm);

    int cb_count = 0;
    message_type send_msg{SIZE,SIZE};
    message_type recv_msg{SIZE,SIZE};

    for (unsigned int i=0; i<SIZE/sizeof(int); ++i)
    {
        send_msg.data<int>()[i] = i*10+comm.rank();
        recv_msg.data<int>()[i] = i*10+comm.rank();
    }

    const auto dst = (comm.rank()+comm.size()+1)%comm.size();
    const auto src = (comm.rank()+comm.size()-1)%comm.size();

    cb_comm.send(send_msg, dst, 0, [&cb_count](int,int,const message_type&){++cb_count;});
    cb_comm.recv(recv_msg, src, 0, [&cb_count](int,int,const message_type&){++cb_count;});

    ok = ok && (cb_comm.pending_sends()==1);
    ok = ok && (cb_comm.pending_recvs()==1);

    auto o_send = cb_comm.detach_send(dst,0);
    auto o_recv = cb_comm.detach_recv(src,0);

    ok = ok && (cb_comm.pending_sends()==0);
    ok = ok && (cb_comm.pending_recvs()==0);
    ok = ok && !cb_comm.progress();
    while (cb_comm.progress()){}
    ok = ok && (cb_count == 0);

    o_send->first.wait();
    o_recv->first.wait();

    for (unsigned int i=0; i<SIZE/sizeof(int); ++i)
        ok = ok && (recv_msg.data<int>()[i] == (int)(i*10+src));

    EXPECT_TRUE(ok);

    MPI_Barrier(comm); 
} 

TEST(detach, detach_cancel_unexpected) 
{
    bool ok = true;
    comm_type     comm;
    callback_comm_type cb_comm(comm);

    message_type send_msg{SIZE,SIZE};
    message_type recv_msg{SIZE,SIZE};
    message_type unexpected_msg;

    for (unsigned int i=0; i<SIZE/sizeof(int); ++i)
    {
        send_msg.data<int>()[i] = i*10+comm.rank();
        recv_msg.data<int>()[i] = i*10+comm.rank();
    }

    const auto dst = (comm.rank()+comm.size()+1)%comm.size();
    const auto src = (comm.rank()+comm.size()-1)%comm.size();
    cb_comm.send(send_msg, dst, 0, [](int,int,const message_type&){ std::cout << "should not be invoked!\n"; });
    cb_comm.recv(recv_msg, src, 1, [](int,int,const message_type&){ std::cout << "should not be invoked!\n"; });

    if (auto o = cb_comm.detach_send(dst,0))
    {
        if (!(o->first.ready()))
            ok = ok && o->first.cancel();
    }
    else 
        ok = false;
    if (auto o = cb_comm.detach_recv(src,1))
    {
        if (!(o->first.ready()))
            ok = ok && o->first.cancel();
    }
    else 
        ok = false;

    ok = ok && !cb_comm.progress();
    while(cb_comm.progress()) {}

    MPI_Barrier(comm); 

    ok = ok && !cb_comm.progress([&unexpected_msg](int,int,const message_type& x){ 
        std::cout << "received unexpected message!\n"; 
        unexpected_msg = x; });
    if (unexpected_msg.size())
    {
        for (unsigned int i=0; i<SIZE/sizeof(int); ++i)
            ok = ok && (unexpected_msg.data<int>()[i] == (int)(i*10+src));
    }

    for (unsigned int i=0; i<SIZE/sizeof(int); ++i)
        ok = ok && (recv_msg.data<int>()[i] == (int)(i*10+comm.rank()));

    EXPECT_TRUE(ok);

    MPI_Barrier(comm); 
}
