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

                struct endpoint_db_mpi
                {
                    using key_t   = ::gridtools::ghex::tl::uuid::type;
                    using value_t = address;

                    MPI_Comm m_mpi_comm;
                    const int m_rank;
                    const int m_size;

                    std::map<key_t,value_t> m_local_endpoints;
                    std::map<key_t,value_t> m_global_endpoints;

                    std::vector<std::vector<key_t>> m_rank_index_lu_table;

                    endpoint_db_mpi(MPI_Comm comm)
                    : m_mpi_comm{comm}
                    , m_rank{ [](MPI_Comm c){ int r; GHEX_CHECK_MPI_RESULT(MPI_Comm_rank(c,&r)); return r; }(comm) }
                    , m_size{ [](MPI_Comm c){ int s; GHEX_CHECK_MPI_RESULT(MPI_Comm_size(c,&s)); return s; }(comm) }
                    , m_rank_index_lu_table(m_size)
                    {}

                    endpoint_db_mpi(const endpoint_db_mpi&) = delete;
                    endpoint_db_mpi(endpoint_db_mpi&&) = default;

                    int rank() const noexcept { return m_rank; }
                    int size() const noexcept { return m_size; }
                    int est_size() const noexcept { return m_size; }

                    value_t* find(key_t k)
                    {
                        auto it = m_global_endpoints.find(k);
                        if (it != m_global_endpoints.end())
                        {
                            return &(it->second);
                        }
                        else
                            return nullptr;
                    }

                    key_t* find(int rank, int index)
                    {
                        const unsigned int r = (unsigned int)rank;
                        const unsigned int i = (unsigned int)index;
                        if (r >= (unsigned int)m_size) return nullptr;
                        if (i >= m_rank_index_lu_table[r].size()) return nullptr;
                        return &(m_rank_index_lu_table[r][i]);
                    }

                    void insert(key_t k, const address& addr)
                    {
                        m_local_endpoints[k] = addr; //, ep.m_ep_h};
                    }

                    void erase(key_t k)
                    {
                        m_local_endpoints.erase(k);
                    }

                    std::vector<key_t> set_diff(std::vector<key_t>& old_list, std::vector<key_t>& new_list)
                    {
                        // remove deleted elements from global map
                        std::vector<key_t> diff;
                        std::set_difference(
                            old_list.begin(), old_list.end(), 
                            new_list.begin(), new_list.end(), 
                            std::inserter(diff, diff.begin()));
                        for (const auto& k : diff)
                        {
                            auto it = m_global_endpoints.find(k);
                            m_global_endpoints.erase(it);
                        }
                        // get new elements
                        diff.clear();
                        std::set_difference(
                            new_list.begin(), new_list.end(), 
                            old_list.begin(), old_list.end(), 
                            std::inserter(diff, diff.begin()));
                        return diff;
                    }

                    void synchronize()
                    {
                        // update database
                        std::vector<key_t> new_key_list;
                        new_key_list.reserve(m_local_endpoints.size());
                        for (const auto& kvp : m_local_endpoints)
                            new_key_list.push_back(kvp.first);

                        for (int r=0; r<m_size; ++r)
                        {
                            if (r==m_rank)
                            {
                                std::size_t size = new_key_list.size();
                                if (m_size > 1)
                                {
                                    GHEX_CHECK_MPI_RESULT(
                                        MPI_Bcast(&size, sizeof(std::size_t), MPI_BYTE, r, m_mpi_comm)
                                    );
                                    GHEX_CHECK_MPI_RESULT(
                                        MPI_Bcast(new_key_list.data(), sizeof(key_t)*size, MPI_BYTE, r, m_mpi_comm)
                                    );
                                }
                                auto diff = set_diff(m_rank_index_lu_table[r], new_key_list);
                                if (m_size > 1)
                                {
                                    for (unsigned int a=0; a<diff.size(); ++a)
                                    {
                                        /*const*/ auto& addr = m_local_endpoints[diff[a]];
                                        size =  addr.size();
                                        GHEX_CHECK_MPI_RESULT(
                                            MPI_Bcast(&size, sizeof(std::size_t), MPI_BYTE, r, m_mpi_comm)
                                        );
                                        GHEX_CHECK_MPI_RESULT(
                                            MPI_Bcast(addr.data(), addr.size(), MPI_BYTE, r, m_mpi_comm)
                                        );
                                    }
                                }
                                for (unsigned int a=0; a<diff.size(); ++a)
                                {
                                    m_global_endpoints[diff[a]] = m_local_endpoints[diff[a]];
                                }

                                m_rank_index_lu_table[r] = new_key_list;
                            }
                            else
                            {
                                std::size_t size;
                                GHEX_CHECK_MPI_RESULT(
                                    MPI_Bcast(&size, sizeof(std::size_t), MPI_BYTE, r, m_mpi_comm)
                                );
                                std::vector<key_t> remote_key_list(size);
                                GHEX_CHECK_MPI_RESULT(
                                    MPI_Bcast(remote_key_list.data(), sizeof(key_t)*size, MPI_BYTE, r, m_mpi_comm)
                                );

                                auto diff = set_diff(m_rank_index_lu_table[r], remote_key_list);
                                for (unsigned int a=0; a<diff.size(); ++a)
                                {
                                    GHEX_CHECK_MPI_RESULT(
                                        MPI_Bcast(&size, sizeof(std::size_t), MPI_BYTE, r, m_mpi_comm)
                                    );
                                    address addr(size);
                                    GHEX_CHECK_MPI_RESULT(
                                        MPI_Bcast(addr.data(), size, MPI_BYTE, r, m_mpi_comm)
                                    );
                                    m_global_endpoints[diff[a]] = addr;
                                }
                                
                                m_rank_index_lu_table[r] = remote_key_list;
                            }
                        }

                        /*if (m_rank == 0)
                        {
                            std::cout << "registered endpoints:\n";
                            for (int r=0; r<m_size; ++r)
                            {
                                std::cout << "rank " << r << "\n";
                                for (const auto& k : m_rank_index_lu_table[r])
                                    std::cout << "  " 
                                    << k << "\n"
                                    << "    " << m_global_endpoints[k] << "\n";
                            }
                            std::cout << std::endl;
                        }*/
                    }

                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_ENDPOINT_DB_MPI_HPP */

