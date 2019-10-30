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

namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace ucx {
                
                struct context;

                struct communicator_base
                {
                    context* m_context;
                    ucp_worker_h m_worker;
                    endpoint m_endpoint;

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
                        ucp_ep_h ep_h;
                        ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
                        ep_params.address    = worker_address;	    
                        GHEX_CHECK_UCX_RESULT(
                            ucp_ep_create(m_worker, &ep_params, &ep_h)
                        );
                        m_endpoint = endpoint{
                            ::gridtools::ghex::tl::uuid::generate(c->m_db.rank()),
                            address{worker_address, address_length}, 
                            ep_h};

                        ucp_worker_release_address(m_worker, worker_address);
                    }
                
                    communicator_base(communicator_base&&) = default;
                    communicator_base(const communicator_base&) = delete;

                    ~communicator_base();
/*                    {
                        m_context->notify_destruction(this);
                        ucp_ep_close_nb(m_endpoint.m_ep_h, UCP_EP_CLOSE_MODE_FLUSH);
                        ucp_worker_destroy(m_worker);
                    }
*/
                    
                    template<class CharT, class Traits = std::char_traits<CharT>>
                    friend std::basic_ostream<CharT,Traits>& operator<<(std::basic_ostream<CharT,Traits>& os, const communicator_base& comm)
                    {
                        os << "communicator_base{" << comm.m_endpoint << "}";
                        return os;
                    }
                    
                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_COMMUNICATOR_BASE_HPP */

