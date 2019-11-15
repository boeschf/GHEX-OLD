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
#ifndef INCLUDED_GHEX_TL_UCX_CONTEXT_HPP
#define INCLUDED_GHEX_TL_UCX_CONTEXT_HPP

#include "./request.hpp"

namespace gridtools{
    namespace ghex {
        namespace tl {
            namespace ucx {

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
                        //context_params.request_size = internal_request::s_padding; 
                        context_params.request_size = sizeof(request)+alignof(request)-1;
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

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_CONTEXT_HPP */

