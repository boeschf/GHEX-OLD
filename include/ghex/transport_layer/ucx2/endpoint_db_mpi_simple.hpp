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
#ifndef INCLUDED_GHEX_TL_UCX_ENDPOINT_DB_MPI_SIMPLE_HPP
#define INCLUDED_GHEX_TL_UCX_ENDPOINT_DB_MPI_SIMPLE_HPP

#include <map>
#include <vector>
#include <algorithm>
#include <iterator>
#include "../mpi/error.hpp"
#include "./error.hpp"
#include "./endpoint.hpp"

#include <iostream>

namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace ucx {

                struct endpoint_db_mpi_simple
                {
                    using key_t   = ::gridtools::ghex::tl::uuid::type;
                    using value_t = address;

                    MPI_Comm m_mpi_comm;
                    const int m_rank;
                    const int m_size;

                    std::map<key_t,value_t> m_local_endpoints;
                    //std::map<key_t,value_t> m_global_endpoints;

                    //std::vector<std::vector<key_t>> m_rank_index_lu_table;

                    endpoint_db_mpi_simple(MPI_Comm comm)
                    : m_mpi_comm{comm}
                    , m_rank{ [](MPI_Comm c){ int r; GHEX_CHECK_MPI_RESULT(MPI_Comm_rank(c,&r)); return r; }(comm) }
                    , m_size{ [](MPI_Comm c){ int s; GHEX_CHECK_MPI_RESULT(MPI_Comm_size(c,&s)); return s; }(comm) }
                    //, m_rank_index_lu_table(m_size)
                    {}

                    endpoint_db_mpi_simple(const endpoint_db_mpi_simple&) = delete;
                    endpoint_db_mpi_simple(endpoint_db_mpi_simple&&) = default;

                    int rank() const noexcept { return m_rank; }
                    int size() const noexcept { return m_size; }
                    int est_size() const noexcept { return m_size; }

                    value_t* find(key_t) { return nullptr; }

                    key_t* find(int, int) { return nullptr; }

                    void insert(key_t, const address&) {}

                    void erase(key_t) {}

                    void synchronize() { }
                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_ENDPOINT_DB_MPI_HPP */

