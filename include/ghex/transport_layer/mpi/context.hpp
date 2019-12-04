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
#ifndef INCLUDED_TL_MPI_CONTEXT_HPP
#define INCLUDED_TL_MPI_CONTEXT_HPP

#include "../context.hpp"
#include "./communicator.hpp"

namespace gridtools {
    namespace ghex {
        namespace tl {

            template<typename ThreadPrimitives>
            struct transport_context<mpi_tag, ThreadPrimitives>
            {
                using communicator_type = communicator<mpi_tag>;

                parallel_context<ThreadPrimitives>& m_parallel_context;

                template<typename... Args>
                transport_context(parallel_context<ThreadPrimitives>& pc, Args&&...)
                : m_parallel_context(pc)
                {}

                communicator_type get_communicator(int) const
                {
                    return {(MPI_Comm)(m_parallel_context.world())};
                }

            };

            //using mpi_context = context<mpi_tag>;

        }
    }
}

#endif /* INCLUDED_TL_MPI_CONTEXT_HPP */


