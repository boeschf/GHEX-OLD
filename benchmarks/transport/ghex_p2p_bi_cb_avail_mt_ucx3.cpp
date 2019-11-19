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

#ifdef USE_MPI
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
using cont_comm_t   = gridtools::ghex::tl::continuation_communicator;
using cont_msg_t    = typename cont_comm_t::message_type;
using cont_future_t = typename cont_comm_t::request;
using msg_t         = gridtools::ghex::tl::message_buffer<>;


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
        const auto peer_rank = (rank+1)%2;    

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
        std::vector<comm_t>   comms;
        std::vector<std::vector<msg_t>> send_msgs(num_threads);
        std::vector<std::vector<msg_t>> recv_msgs(num_threads);
        //std::vector<int> comm_cnt(num_threads, 0);
        //std::vector<int> nlsend_cnt(num_threads, 0);
        //std::vector<int> nlrecv_cnt(num_threads, 0);
        //std::vector<int> submit_cnt(num_threads, 0);
        //std::vector<int> submit_recv_cnt(num_threads, 0);
        std::vector<std::vector<cont_future_t>> send_reqs(num_threads);
        std::vector<std::vector<cont_future_t>> recv_reqs(num_threads);
        for (int i=0; i<num_threads; ++i)
        {
#ifndef USE_MPI
            comms.push_back(context.make_communicator());
#else
            comms.push_back(comm_t());
#endif
            for (int j=0; j<inflight; ++j)
            {

                send_msgs[i].push_back(msg_t(buff_size));
                recv_msgs[i].push_back(msg_t(buff_size));
                send_msgs[i].back().data<char>()[0] = 0;
                recv_msgs[i].back().data<char>()[0] = 0;
                send_reqs[i].push_back(cont_future_t());
                recv_reqs[i].push_back(cont_future_t());
            }
        }

        timer.tic();
        ttimer.tic();
        #pragma omp parallel
        {
            const auto thread_id = omp_get_thread_num();
            
            int dbg = 0, sdbg = 0, rdbg = 0;
            while (sent < niter || received < niter)
            {
                if (rank==0 && thread_id==0 && sdbg>=(niter/10))
                {
                    std::cout << sent << " sent\n";
                    sdbg = 0;
                }

                if (rank==0 && thread_id==0 && rdbg>=(niter/10))
                {
                    std::cout << received << " received\n";
                    rdbg = 0;
                }
                
                if(thread_id == 0 && dbg >= (niter/10))
                {
                    dbg = 0;
                    const double bytes = ((received-last_received + sent-last_sent)*size*buff_size)/2;
                    std::cout << rank << " total bwdt MB/s:      " << bytes/timer.toc() << "\n";
                    timer.tic();
                    last_received = received;
                    last_sent = sent;
                }
                
                for(int j=0; j<inflight; j++)
                {
                    if(recv_reqs[thread_id][j].is_ready())
                    {
                        //submit_recv_cnt[thread_id] += num_threads;
                        rdbg += num_threads;
                        dbg  += num_threads;

                        recv_reqs[thread_id][j] = cont_comm.recv(
                            comms[thread_id],
                            recv_msgs[thread_id][j], peer_rank, thread_id*inflight+j,
                            [](cont_msg_t, int, int)
                            {
                                ++received;
                            });
                    }
                    else cont_comm.progress();

                    if(sent < niter && send_reqs[thread_id][j].is_ready())
                    {
                        //submit_cnt[thread_id] += num_threads;
                        sdbg += num_threads;
                        dbg  += num_threads;

                        send_reqs[thread_id][j] = cont_comm.send(
                            comms[thread_id],
                            send_msgs[thread_id][j], peer_rank, thread_id*inflight+j,
                            [](cont_msg_t, int, int)
                            {
                                ++sent;
                            });
                    }
                    else cont_comm.progress();
                }
            }


    /*while(sent < niter || received < niter){

	    for(int j=0; j<inflight; j++){
		if(rmsgs[j].use_count() == 1){
		    submit_recv_cnt += nthr;
		    rdbg += nthr;
		    dbg += nthr;
		    comm.recv(rmsgs[j], peer_rank, thrid*inflight+j, recv_callback);
		} else comm.progress();

		if(sent < niter && smsgs[j].use_count() == 1){
		    submit_cnt += nthr;
		    sdbg += nthr;
		    dbg += nthr;
		    comm.send(smsgs[j], peer_rank, thrid*inflight+j, send_callback);
		} else comm.progress();
	    }
	}i*/

        }

        if(rank == 1)
        {
            const double bytes = niter*size*buff_size;
            const auto time = ttimer.toc();
            std::cout << "time:            " << time/1000000 << "s\n";
            std::cout << "final MB/s:      " << bytes/time << "\n";
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
}