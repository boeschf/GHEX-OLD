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
#ifndef INCLUDED_GHEX_TL_UCX_ENVIRONMENT_HPP
#define INCLUDED_GHEX_TL_UCX_ENVIRONMENT_HPP

#include <ucp/api/ucp.h>
#include <map>
#include <tuple>
#include <memory>
#include <cstring>
#include <stdio.h>
#include <iostream>
#include "./error.hpp"

namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace ucx {

                /*struct request
                {
                    ucp_worker_h m_worker;
                };*/

                struct internal_request
                {
                    unsigned char* m_data = nullptr;

                    ~internal_request()
                    {
                        if (m_data)
                            delete[] m_data;
                    }

                    internal_request() noexcept {}

                    internal_request(std::size_t size_)
                    : m_data{ size_>0u? new unsigned char[size_] : nullptr }
                    {}

                    internal_request(const internal_request&) = delete;

                    internal_request(internal_request&& other) noexcept
                    : m_data{ other.m_data }
                    {
                        other.m_data = nullptr;
                    }
                };

                struct context
                {
                    ucp_context_h m_context;
                    std::size_t m_req_size;

                    context(int est_size)
                    {
                        // read run-time context
                        ucp_config_t* config_ptr;
                        GHEX_CHECK_UCX_RESULT(
                            ucp_config_read(NULL,NULL, &config_ptr)
                        );

                        // set parameters
                        ucp_params_t context_params;
                        // define valid fields
                        context_params.field_mask =
                            UCP_PARAM_FIELD_FEATURES          | // features
                            UCP_PARAM_FIELD_REQUEST_SIZE      | // size of reserved space in a non-blocking request
                            UCP_PARAM_FIELD_TAG_SENDER_MASK   | // mask which gets sender endpoint from a tag
                            UCP_PARAM_FIELD_MT_WORKERS_SHARED | // multi-threaded context: thread safety
                            UCP_PARAM_FIELD_ESTIMATED_NUM_EPS ; // estimated number of endpoints for this context
                        // features
                        context_params.features =
                            UCP_FEATURE_TAG                   ; // tag matching
                        // request size
                        //context_params.request_size = sizeof(request);
                        context_params.request_size = 0;
                        // thread safety
                        context_params.mt_workers_shared = false;
                        // estimated number of connections
                        context_params.estimated_num_eps = est_size;
                        // mask
                        context_params.tag_sender_mask  = 0x00000000fffffffful;

                        GHEX_CHECK_UCX_RESULT(
                            ucp_init(&context_params, config_ptr, &m_context)
                        );

                        ucp_config_release(config_ptr);
	    
                        // ask for UCP request size
		                ucp_context_attr_t attr;
		                attr.field_mask = UCP_ATTR_FIELD_REQUEST_SIZE;
		                ucp_context_query(m_context, &attr);
                        m_req_size = attr.request_size;
                        std::cout << "request size is " << attr.request_size << std::endl;
                    }

                    context(const context&) = delete;
                    context(context&&) noexcept = default;

                    ~context()
                    {
                        ucp_cleanup(m_context);
                    }

                    operator       ucp_context_h&()       noexcept { return m_context; }
                    operator const ucp_context_h&() const noexcept { return m_context; }

                };

                struct address
                {
                    struct address_array_deleter
                    {
                        void operator()(unsigned char* ptr) const
                        {
                            delete[] ptr;
                        }
                    };
                    std::unique_ptr<unsigned char[], address_array_deleter> m_address_array;
                    std::size_t m_length;

                    address()
                    : m_length{0u}
                    {}

                    address(const ucp_address_t* address_handle, std::size_t address_length)
                    : m_address_array{ new unsigned char[address_length] }
                    , m_length{ address_length }
                    {
                        std::memcpy( m_address_array.get(), address_handle,  address_length);
                    }

                    address(const address&) = delete;
                    address(address&&) noexcept = default;
                    address& operator=(const address&) = delete;
                    address& operator=(address&&) noexcept = default;

                    const ucp_address_t* get() const noexcept { return reinterpret_cast<const ucp_address_t*>(m_address_array.get()); }
                    ucp_address_t*       get()       noexcept { return reinterpret_cast<ucp_address_t*>(m_address_array.get()); }
                };

                struct communicator_impl
                {
                    using rank_type = int;
                    using size_type = int;

                    rank_type m_rank;
                    size_type m_size;
                    context m_context;
                    std::map<rank_type, std::tuple<address, ucp_ep_h>> m_map;
                    ucp_worker_h m_worker;
                    
                    template<typename MPIComm>
                    communicator_impl(const MPIComm& comm)
                    : m_rank{ comm.rank() }
                    , m_size{ comm.size() }
                    , m_context{ m_size }
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
                            std::cout << "  worker address is length " << address_length << " (rank " << m_rank << ")" << std::endl;
                            ucp_ep_params_t ep_params;
                            ucp_ep_h ep_h;
                            ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
                            ep_params.address    = worker_address;	    
                            GHEX_CHECK_UCX_RESULT(
                                ucp_ep_create(m_worker, &ep_params, &ep_h)
                            );
                            //std::cout << "my endpoint: " << std::endl;
                            //std::cout << "my endpoint: (I am rank " << m_rank << ")" << std::endl;
                            //ucp_ep_print_info(ep_h, stdout);
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

                            std::cout << "  all addresses" << std::endl;
                            for (auto& x : all_addresses)
                            {
                                for (auto c : x)
                                    std::cout << std::hex << (int)c;
                                std::cout << std::endl;
                            }
                            std::cout << std::dec;

                            for (rank_type r=0; r<m_size; ++r)
                            {
                                if (r==m_rank) continue;
                                ucp_address_t* remote_worker_address = reinterpret_cast<ucp_address_t*>( all_addresses[r].data() );
                                //std::size_t remote_worker_address_length = all_lengths[r];
                                ucp_ep_params_t ep_params;
                                ucp_ep_h ep_h;
                                ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
                                ep_params.address    = remote_worker_address;	    
                                GHEX_CHECK_UCX_RESULT(
                                    ucp_ep_create(m_worker, &ep_params, &ep_h)
                                );
                            //std::cout << "other endpoint: (I am rank " << m_rank << ")" << std::endl;
                            //ucp_ep_print_info(ep_h, stdout);


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

                        std::cout << "address lookup table has size " << m_map.size() << std::endl;
                    }

                    ~communicator_impl()
                    {
                        for (auto& x : m_map)
                        {
                            //if (x.first == m_rank) continue;
                            ucp_ep_close_nb(std::get<1>(x.second), UCP_EP_CLOSE_MODE_FLUSH);
                        }
                    }
                };

                struct communicator_base
                {
                    std::shared_ptr<communicator_impl> m_impl;

                    template<typename MPIComm>
                    communicator_base(const MPIComm& comm)
                    : m_impl{ new communicator_impl{comm} }
                    { }

                    communicator_base(const communicator_base&) = default;
                    communicator_base(communicator_base&&) = default;


                    void test_send_recv()
                    {
                        int send_payload = m_impl->m_rank+100;
                        int next_rank = (m_impl->m_rank+1)%m_impl->m_size;
                        int prev_rank = (m_impl->m_rank+m_impl->m_size-1)%m_impl->m_size;

                        int recv_payload = -9999;

                        // allocate memory for the request
                        internal_request req_send(m_impl->m_context.m_req_size);
                        internal_request req_recv(m_impl->m_context.m_req_size);
                        
                        const auto send_tag = (std::uint_fast64_t{99} << 32) | (std::uint_fast64_t)(m_impl->m_rank);
                        const auto recv_tag = (std::uint_fast64_t{99} << 32) | (std::uint_fast64_t)(prev_rank);

                        auto next_ep = std::get<1>(m_impl->m_map[next_rank]);
                        //auto prev_ep = std::get<1>(m_impl->m_map[prev_rank]);

                        std::cout << "sending message from " << m_impl->m_rank << " to   " << next_rank << " with tag " << send_tag << std::endl; 
                        auto send_status = ucp_tag_send_nbr(
                            next_ep, 
                            &send_payload, sizeof(int), ucp_dt_make_contig(1), 
                            send_tag, 
                            req_send.m_data + m_impl->m_context.m_req_size);
                        std::cout << "recveing message at  " << m_impl->m_rank << " from " << prev_rank << " with tag " << recv_tag << std::endl; 
                        auto recv_status = ucp_tag_recv_nbr(
                            m_impl->m_worker, 
                            &recv_payload, sizeof(int), ucp_dt_make_contig(1), 
                            recv_tag, 
                            //0xffffffff00000000ul,
                            0xfffffffffffffffful,
                            req_recv.m_data + m_impl->m_context.m_req_size);

                        if (send_status != UCS_OK && send_status != UCS_INPROGRESS)
                        {
                            std::cout << "something went wrong when sending!!!! err code = " << send_status << std::endl;
                        }
                        if (recv_status != UCS_OK && recv_status != UCS_INPROGRESS)
                        {
                            std::cout << "something went wrong when receiving!!!! err code = " << recv_status << std::endl;
                        }

                        std::cout << "progressing worker..." << std::endl;
                        while (send_status == UCS_INPROGRESS)
                        {
                            ucp_worker_progress(m_impl->m_worker);
                            send_status = ucp_request_check_status(req_send.m_data + m_impl->m_context.m_req_size);
                        }
                        while (recv_status == UCS_INPROGRESS)
                        {
                            ucp_worker_progress(m_impl->m_worker);
                            recv_status = ucp_request_check_status(req_recv.m_data + m_impl->m_context.m_req_size);
                        }
                        if (send_status != UCS_OK)
                        {
                            std::cout << "final send status not ok!!" << std::endl;
                        }
                        if (recv_status != UCS_OK)
                        {
                            std::cout << "final recv status not ok!!" << std::endl;
                        }

                        std::cout << "received the following message : " << recv_payload << " from " << prev_rank << std::endl;
                    }

                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_ENVIRONMENT_HPP */

