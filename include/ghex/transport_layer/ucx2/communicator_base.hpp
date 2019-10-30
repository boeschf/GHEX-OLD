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

                    int rank() const;
                    int size() const;
                    
                    endpoint connect(uuid_t id);
                    endpoint connect(int rank, int index);
                    auto get_id(int rank, int index);
                    
                    /*template<typename Message>
                    int send(const Message& msg, uuid_t id)
                    {
                        return send(msg,connect(id));
                    }*/
                    
                    /*template<typename Message>
                    int send(const Message& msg, int rank, int index)
                    {
                        return send(msg,connect(rank,index));
                    }*/
                    
                    template<typename Message>
                    int send(const Message& msg, endpoint ep)
                    {
                        // send with tag = uuid
                        return 1; 
                    }

                    /*template<typename Message>
                    int recv(Message& msg, int rank, int index)
                    {
                        if(auto ptr = get_id(rank,index))
                            return recv(msg, *ptr);
                        else 
                            return 0;
                    }*/

                    template<typename Message>
                    int recv(Message& msg, endpoint ep)
                    {
                        // match tag to ep.m_id
                        return 2;
                    }

                    /*template<typename Message>
                    int recv(Message& msg, uuid_t id)
                    {
                        // match tag to id
                        return 2;
                    }*/
                    
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

