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
#ifndef INCLUDED_GHEX_TL_UCX_COMMUNICATOR_BASE_HPP
#define INCLUDED_GHEX_TL_UCX_COMMUNICATOR_BASE_HPP

#include "./address.hpp"
#include "./context.hpp"
#include "../../allocator/pool_allocator_adaptor.hpp"

#include <map>
#include <tuple>


namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace ucx {

                template<typename Allocator>
                struct communicator_impl
                {
                    using rank_type = int;
                    using size_type = int;

                    rank_type m_rank;
                    size_type m_size;
                    context m_context;
                    std::map<rank_type, std::tuple<address, ucp_ep_h>> m_map;
                    ucp_worker_h m_worker;

                    using pool_type = ::gridtools::ghex::allocator::pool<Allocator>;
                    using allocator_type = typename pool_type::allocator_type;
                    using request_type = internal_request<allocator_type>;

                    pool_type m_request_pool;
                    
                    template<typename MPIComm>
                    communicator_impl(const MPIComm& comm)
                    : m_rank{ comm.rank() }
                    , m_size{ comm.size() }
                    , m_context{ m_size }
                    , m_request_pool{ Allocator{} }
                    {
                        ucp_worker_params_t params;
                        /* this should not be used if we have a single worker per thread */
		                params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
                        #ifdef GHEX_UCX_THREAD_MODE_MULTIPLE
		                wparams.thread_mode = UCS_THREAD_MODE_MULTI;
                        #else
		                params.thread_mode = UCS_THREAD_MODE_SINGLE;
                        #endif
	                    GHEX_CHECK_UCX_RESULT(
		                    ucp_worker_create (m_context, &params, &m_worker)
                        );

                        {
                            ucp_address_t* worker_address;
                            std::size_t address_length;
	                        GHEX_CHECK_UCX_RESULT(
                                ucp_worker_get_address(m_worker, &worker_address, &address_length)
                            );
                            ucp_ep_params_t ep_params;
                            ucp_ep_h ep_h;
                            ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
                            ep_params.address    = worker_address;	    
                            GHEX_CHECK_UCX_RESULT(
                                ucp_ep_create(m_worker, &ep_params, &ep_h)
                            );
                            m_map.emplace( std::make_pair( m_rank, std::make_tuple(address{worker_address, address_length}, ep_h) ) );
                            ucp_worker_release_address(m_worker, worker_address);
                        }

                        if (m_size > 1)
                        {
                            // all_gather_v is needed for out of band address exchange!
                            auto my_address    = std::get<0>(m_map[m_rank]).m_address_array.get();
                            auto my_length     = std::get<0>(m_map[m_rank]).m_length;
                            auto all_lengths   = comm.all_gather(my_length).get();
                            auto all_addresses = comm.all_gather(my_address, all_lengths).get();

                            for (rank_type r=0; r<m_size; ++r)
                            {
                                if (r==m_rank) continue;
                                ucp_address_t* remote_worker_address = reinterpret_cast<ucp_address_t*>( all_addresses[r].data() );
                                ucp_ep_params_t ep_params;
                                ucp_ep_h ep_h;
                                ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
                                ep_params.address    = remote_worker_address;	    
                                GHEX_CHECK_UCX_RESULT(
                                    ucp_ep_create(m_worker, &ep_params, &ep_h)
                                );

                                m_map.emplace(
                                    std::make_pair(
                                        r, 
                                        std::make_tuple(
                                            address{
                                                reinterpret_cast<const ucp_address_t*>( all_addresses[r].data() ),
                                                all_lengths[r]
                                            },
                                            ep_h
                                        ) 
                                    )
                                );
                            }
                        }

                    }

                    ~communicator_impl()
                    {
                        for (auto& x : m_map)
                        {
                            ucp_ep_close_nb(std::get<1>(x.second), UCP_EP_CLOSE_MODE_FLUSH);
                        }
                    }
                };

                struct communicator_base
                {
                    using impl_type      = communicator_impl<std::allocator<unsigned char>>;
                    using allocator_type = typename impl_type::allocator_type;
                    using request_type   = typename impl_type::request_type;
                    using rank_type      = typename impl_type::rank_type;
                    using size_type      = typename impl_type::size_type;
                    using tag_type       = int;
                    using address_type   = address;

                    std::shared_ptr< impl_type > m_impl;

                    template<typename MPIComm>
                    communicator_base(const MPIComm& comm)
                    : m_impl{ new impl_type{comm} }
                    { }

                    communicator_base(const communicator_base&) = default;
                    communicator_base(communicator_base&&) = default;

                    /** @return rank of this process */
                    inline rank_type rank() const noexcept { return m_impl->m_rank; }
                    /** @return size of communicator group*/
                    inline size_type size() const noexcept { return m_impl->m_size; }
                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_COMMUNICATOR_BASE_HPP */

