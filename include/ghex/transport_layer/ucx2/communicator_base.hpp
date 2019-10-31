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

#include <ucp/api/ucp.h>
#include "./endpoint.hpp"
#include "./endpoint_info.hpp"
#include "./request.hpp"
#include <iostream>

namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace ucx {
                
                struct context;

                struct communicator_base
                {
                    using uuid_t = gridtools::ghex::tl::uuid::type;

                    context* m_context;
                    ucp_worker_h m_worker;
                    address m_address;
                    endpoint m_ep;

                    std::map<uuid_t, endpoint> m_ep_cache;

                    template<typename Context>
                    communicator_base(Context* c)
                    {
                        ucp_worker_params_t params;
		                params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
		                params.thread_mode = UCS_THREAD_MODE_SINGLE;
	                    GHEX_CHECK_UCX_RESULT(
		                    ucp_worker_create (c->m_context, &params, &m_worker)
                        );
                        ucp_address_t* worker_address;
                        std::size_t address_length;
	                    GHEX_CHECK_UCX_RESULT(
                            ucp_worker_get_address(m_worker, &worker_address, &address_length)
                        );
                        ucp_ep_params_t ep_params;
                        ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
                        ep_params.address    = worker_address;	    
                        GHEX_CHECK_UCX_RESULT(
                            ucp_ep_create(m_worker, &ep_params, &m_ep.m_ep_h)
                        );
                        m_ep.m_id = ::gridtools::ghex::tl::uuid::generate(c->m_db.rank());
                        m_address = address{worker_address, address_length};
                        ucp_worker_release_address(m_worker, worker_address);
                    }
                
                    communicator_base(communicator_base&& other);

                    communicator_base(const communicator_base&) = delete;

                    ~communicator_base();

                    endpoint_info packed_endpoint() const { return {m_ep.m_id, m_address}; }

                    int rank() const;
                    int size() const;
                    
                    endpoint connect(uuid_t id);
                    endpoint connect(int rank, int index);
                    endpoint connect(const endpoint_info& info)
                    {
                        uuid_t id;
                        address addr; 
                        info.unpack(id, addr);
                        auto it = m_ep_cache.find(id);
                        if (it != m_ep_cache.end())
                            return it->second;
                        ucp_address_t* remote_worker_address = addr.get();
                        ucp_ep_params_t ep_params;
                        ucp_ep_h ep_h;
                        ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
                        ep_params.address    = remote_worker_address;	    
                        GHEX_CHECK_UCX_RESULT(
                            ucp_ep_create(m_worker, &ep_params, &ep_h)
                        );
                        m_ep_cache[id] = endpoint{id, ep_h};
                        return {id, ep_h};
                    }
                    auto get_id(int rank, int index);
                    
                    static void empty_send_callback(void *, ucs_status_t) {}
                    static void empty_recv_callback(void *, ucs_status_t, ucp_tag_recv_info_t*) {}
                    
                    template<typename Message>
                    request send(const Message& msg, endpoint ep)
                    {
                        // send with tag = uuid
                        ucs_status_ptr_t ret = ucp_tag_send_nb(
                            ep.m_ep_h,                                       // destination
                            msg.data(),                                      // buffer
                            msg.size()*sizeof(typename Message::value_type), // buffer size
                            ucp_dt_make_contig(1),                           // data type
                            m_ep.m_id,                                       // tag: my uuid
                            &communicator_base::empty_send_callback);        // callback function pointer: empty here
                        if (reinterpret_cast<std::uintptr_t>(ret) == UCS_OK)
                        {
                            // send operation is completed immediately and the call-back function is not invoked
                            return {};
                        } 
                        else if(!UCS_PTR_IS_ERR(ret))
                        {
                            return {(void*)ret, m_worker};
                        }
                        else
                        {
                            // an error occurred
                            throw std::runtime_error("ghex: ucx error - send operation failed");
                        }
                    }

                    template<typename Message>
                    request recv(Message& msg, endpoint ep)
                    {
                        // match tag to ep.m_id
                        ucs_status_ptr_t ret = ucp_tag_recv_nb(
                            m_worker,                                        // worker
                            msg.data(),                                      // buffer
                            msg.size()*sizeof(typename Message::value_type), // buffer size
                            ucp_dt_make_contig(1),                           // data type
                            ep.m_id,                                         // tag: sender uuid
                            ~uuid_t(0ul),                                    // tag mask
                            &communicator_base::empty_recv_callback);        // callback function pointer: empty here
                        if(!UCS_PTR_IS_ERR(ret))
                        {
                            return {(void*)ret, m_worker};
                        }
                        else
                        {
                            // an error occurred
                            throw std::runtime_error("ghex: ucx error - recv operation failed");
                        }
                    }
                    
                    template<class CharT, class Traits = std::char_traits<CharT>>
                    friend std::basic_ostream<CharT,Traits>& operator<<(std::basic_ostream<CharT,Traits>& os, const communicator_base& comm)
                    {
                        os << "communicator_base{" << comm.m_ep.m_id << ", " << comm.m_address << "}";
                        return os;
                    }
                    
                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_COMMUNICATOR_BASE_HPP */

