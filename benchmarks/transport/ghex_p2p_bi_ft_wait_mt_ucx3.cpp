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
#include <vector>
#include <atomic>
#include <omp.h>

//#include <ghex/transport_layer/ucx/threads.hpp>
#include <ghex/common/timer.hpp>
//#include "utils.hpp"

//#define USE_MPI

#ifdef USE_MPI
#ifndef THREAD_MODE_MULTIPLE
#define THREAD_MODE_MULTIPLE
#endif
/* MPI backend */
#include <ghex/transport_layer/mpi/communicator.hpp>
using comm_t      = gridtools::ghex::tl::communicator<gridtools::ghex::tl::mpi_tag>;
using future_t    = gridtools::ghex::tl::communicator<gridtools::ghex::tl::mpi_tag>::future<void>;
#else
/* UCX backend */
#include <ghex/transport_layer/ucx3/address_db_mpi.hpp>
#include <ghex/transport_layer/ucx3/context.hpp>
using db_t        = gridtools::ghex::tl::ucx::address_db_mpi;
using context_t   = gridtools::ghex::tl::context<gridtools::ghex::tl::ucx_tag>;
using comm_t      = typename context_t::communicator_type;
using future_t    = typename comm_t::future;
#endif /* USE_MPI */

#include <ghex/transport_layer/continuation_communicator.hpp>
#include <ghex/transport_layer/message_buffer.hpp>
using cont_comm_t = gridtools::ghex::tl::continuation_communicator;
using msg_t       = gridtools::ghex::tl::message_buffer<>;


std::atomic<int> sent(0);
std::atomic<int> received(0);
int last_received = 0;
int last_sent = 0;

int main(int argc, char *argv[])
{
    gridtools::ghex::timer timer, ttimer;

    int mode;
#ifdef THREAD_MODE_MULTIPLE
    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &mode);
    if(mode != MPI_THREAD_MULTIPLE)
    {
	    std::cerr << "MPI_THREAD_MULTIPLE not supported by MPI, aborting\n";
	    std::terminate();
    }
#else
    MPI_Init_thread(NULL, NULL, MPI_THREAD_SINGLE, &mode);
#endif
    
    if(argc != 4)
    {
        std::cerr << "Usage: bench [niter] [msg_size] [inflight]" << "\n";
        std::terminate();
    }
    
    const int niter = atoi(argv[1]);
    const std::size_t buff_size = atoi(argv[2]);
    const int inflight = atoi(argv[3]);   

    {

#ifndef USE_MPI
        context_t context{ db_t{MPI_COMM_WORLD} };
        comm_t comm = context.make_communicator();
#else
        comm_t comm;
#endif

        const auto rank = comm.rank();
        const auto size = comm.size();
        const auto peer_rank = (rank+1)%size;    

        if(rank==0)
            std::cout << "\n\nrunning test " << __FILE__ << " with communicator " << typeid(comm).name() << "\n\n";

        int num_threads;
        #pragma omp parallel
        {
            #pragma omp master
            {
                num_threads = omp_get_num_threads();
            }
        }

        if(rank == 0) 
            std::cout << "number of threads: " << num_threads << std::endl;
        
        // shared callback communicator
        cont_comm_t cont_comm;

        // per-thread objects
//        std::vector<std::vector<msg_t>> send_msgs(num_threads);
//        std::vector<std::vector<msg_t>> recv_msgs(num_threads);
//        std::vector<std::vector<future_t>> send_reqs(num_threads);
//        std::vector<std::vector<future_t>> recv_reqs(num_threads);
        std::vector<comm_t>   comms;
        for (int i=0; i<num_threads; ++i)
        {
#ifndef USE_MPI
            comms.push_back(context.make_communicator());
#else
            comms.push_back(comm_t());
#endif
//            for (int j=0; j<inflight; ++j)
//            {
//
//                send_msgs[i].push_back(msg_t(buff_size));
//                recv_msgs[i].push_back(msg_t(buff_size));
//                send_msgs[i].back().data<char>()[0] = 0;
//                recv_msgs[i].back().data<char>()[0] = 0;
//                send_reqs[i].push_back(future_t());
//                recv_reqs[i].push_back(future_t());
//            }
        }

        //std::cout << "starting main loop" << std::endl;

        timer.tic();
        ttimer.tic();
        #pragma omp parallel default(none), shared(std::cout,timer,comms,inflight,buff_size,num_threads,niter,peer_rank)
        {
            const auto thread_id = omp_get_thread_num();

            auto comm = comms[thread_id];

            std::vector<msg_t> send_msgs;
            std::vector<msg_t> recv_msgs;
            std::vector<future_t> send_reqs(inflight);
            std::vector<future_t> recv_reqs(inflight);
            for (int j=0; j<inflight; ++j)
            {
                send_msgs.push_back(msg_t(buff_size));
                recv_msgs.push_back(msg_t(buff_size));
                send_msgs.back().data<char>()[0] = 0;
                recv_msgs.back().data<char>()[0] = 0;
            }
            
            int dbg = 0;//, sdbg = 0, rdbg = 0;
            for (int ii=0; ii<niter/(inflight*num_threads); ++ii)
            {
                    if(thread_id == 0 && dbg >= (niter/(inflight*num_threads))/(10))
                    {
                        dbg = 0;
                        const auto time = timer.vtoc();
                        std::cout << time/1000000 << "\n";
                        timer.tic();
                        /*const double bytes = ((received-last_received + sent-last_sent)*size*buff_size)/2;
                        std::cout << rank << " total bwdt MB/s:      " << bytes/timer.toc() << "\n";
                        timer.tic();
                        last_received = received;
                        last_sent = sent;*/
                    }
                ++dbg;

                for(int j=0; j<inflight; j++)
                {
		
                    //dbg      += num_threads;
		            //sent     += num_threads;
		            //received += num_threads;
                        
                    recv_reqs[j] = comm.recv(recv_msgs[j], peer_rank, thread_id*inflight + j);
                    //send_reqs[j] = comm.send(send_msgs[j], peer_rank, thread_id*inflight + j);
                }
                for(int j=0; j<inflight; j++)
                    send_reqs[j] = comm.send(send_msgs[j], peer_rank, thread_id*inflight + j);
	    
                /* wait for all */
	            for(int j=0; j<inflight; j++)
                {
                    send_reqs[j].wait();
                    //recv_reqs[j].wait();
	            }
	            for(int j=0; j<inflight; j++)
                    recv_reqs[j].wait();
            }
        }
        
        if(rank == 1)
        {
            const double bytes = (niter/(inflight*num_threads))*(inflight*num_threads)*size*buff_size;
            const auto time = ttimer.toc();
            std::cout << "time:            " << time/1000000 << "s\n";
            std::cout << "final MB/s:      " << bytes/time << "\n";
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
}
