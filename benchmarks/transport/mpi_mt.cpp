#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <iostream>
#include <unistd.h>
#include <mpi.h>
#include <omp.h>
#include "tictoc.h"

int main(int argc, char *argv[])
{
    int rank, size, threads, peer_rank;
    int niter, buff_size;
    int inflight;
    MPI_Comm mpi_comm;
    int ncomm;

    niter = atoi(argv[1]);
    buff_size = atoi(argv[2]);
    inflight = atoi(argv[3]);
    
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &threads);
    MPI_Comm_dup(MPI_COMM_WORLD, &mpi_comm);
    MPI_Comm_rank(mpi_comm, &rank);
    MPI_Comm_size(mpi_comm, &size);
    if(size!=2){
	fprintf(stderr, "ERROR: only works for 2 MPI ranks\n");
	exit(1);
    }
    peer_rank = (rank+1)%2;

    if(rank==0)	std::cout << "\n\nrunning test " << __FILE__ << "\n\n";

#pragma omp parallel shared(niter, buff_size, peer_rank) reduction( + : ncomm )
    {
	int thrid, nthr;
	unsigned char **buffers = new unsigned char *[inflight];
	MPI_Request *req = new MPI_Request[inflight];
	
	thrid = omp_get_thread_num();
	nthr = omp_get_num_threads();

	fprintf(stderr, "rank %d thrid %d started\n", rank, thrid);

	for(int j=0; j<inflight; j++){
	    req[j] = MPI_REQUEST_NULL;
	    MPI_Alloc_mem(buff_size, MPI_INFO_NULL, &buffers[j]);	
	    for(int i=0; i<buff_size; i++) {
		buffers[j][i] = i%(rank+thrid+1);
	    }
	}
	
#pragma omp master
	if(rank == 1) {
	    tic();
	    bytes = (double)niter*buff_size;
	}

	int i = 0, dbg = 0;
	ncomm = 0;
	while(i<niter){

	    /* submit inflight async requests */
	    for(int j=0; j<inflight; j++){
		if(rank==0 && thrid==0 && dbg>=(niter/10)) {fprintf(stderr, "%d iters\n", i); dbg=0;}
		if(rank==0)
		    MPI_Isend(buffers[j], buff_size, MPI_BYTE, peer_rank, thrid*inflight+j, mpi_comm, &req[j]);
		else
		    MPI_Irecv(buffers[j], buff_size, MPI_BYTE, peer_rank, thrid*inflight+j, mpi_comm, &req[j]);
		dbg +=nthr; i+=nthr; if(i==niter) break;
	    }

	    /* wait for all to complete */
	    MPI_Waitall(inflight, req, MPI_STATUS_IGNORE);

	    /* required in MT for performance. */
	    if(nthr>2) sched_yield();
	}

#pragma omp master
	if(rank == 1) toc();	
    }

    printf("rank %d ncomm %d\n", rank, ncomm);    
    MPI_Finalize();
}
