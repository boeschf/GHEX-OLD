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
#ifndef INCLUDED_GHEX_TL_UCX_ENDPOINT_DB_MPI_HPP
#define INCLUDED_GHEX_TL_UCX_ENDPOINT_DB_MPI_HPP

#include "../mpi/error.hpp"
#include "./error.hpp"
#include "./endpoint.hpp"

namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace ucx {

                struct endpoint_db_mpi
                {
                    MPI_Comm m_mpi_comm;
                    const int m_rank;
                    const int m_size;

                    endpoint_db_mpi(MPI_Comm comm)
                    : m_mpi_comm{comm}
                    , m_rank{ [](MPI_Comm c){ int r; GHEX_CHECK_MPI_RESULT(MPI_Comm_rank(c,&r)); return r; }(comm) }
                    , m_size{ [](MPI_Comm c){ int s; GHEX_CHECK_MPI_RESULT(MPI_Comm_size(c,&s)); return s; }(comm) }
                    {}

                    endpoint_db_mpi(const endpoint_db_mpi&) = delete;
                    endpoint_db_mpi(endpoint_db_mpi&&) = default;

                    int rank() const noexcept { return m_rank; }
                    int size() const noexcept { return m_size; }
                    int est_size() const noexcept { return m_size; }

                    void insert_endpoint(const endpoint& ep)
                    {
                    }

                    void erase_endpoint(const endpoint& ep)
                    {
                    }

                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_ENDPOINT_DB_MPI_HPP */

