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
#ifndef INCLUDED_GHEX_TL_UCX_REQUEST_STATE_HPP
#define INCLUDED_GHEX_TL_UCX_REQUEST_STATE_HPP

#include <memory>
#include "./error.hpp"

namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace ucx {

                struct worker_t;

                struct shared_request_state
                {
                    //std::size_t m_ref_count = 0u;
                    worker_t*   m_worker;
                    void*       m_ptr = nullptr; 
                    bool        m_enqueued = false;
                };

                /*struct shared_request
                {
                    shared_request_state* m_state = nullptr;

                    shared_request() = default;

                    shared_request(shared_request_state* state)
                    : m_state{state}
                    {
                        ++(m_state->m_ref_count);
                    }
                    
                    shared_request(const shared_request& other)
                    : m_state{other.m_state}
                    {
                        if (m_state)
                            ++(m_state->m_ref_count);
                    }
                    
                    shared_request(shared_request&& other)
                    : m_state{other.m_state}
                    {
                        other.m_state = nullptr;
                    }

                    shared_request& operator=(const shared_request& other)
                    {
                        destroy();
                        m_state = other.m_state;
                        if (m_state)
                            ++(m_state->m_ref_count);
                        return *this;
                    }

                    shared_request& operator=(shared_request&& other)
                    {
                        destroy();
                        m_state = other.m_state;
                        other.m_state = nullptr;
                        return *this;
                    }

                    ~shared_request()
                    {
                        destroy();
                    }

                    void destroy()
                    {
                        if (m_state)
                        {
                            if (m_state->m_ref_count == 1u)
                                delete m_state;
                            else
                                --(m_state->m_ref_count);
                        }
                    }

                    shared_request_state* get() { return m_state; }
                };*/

                struct request2
                {
                    using data_type = std::shared_ptr<shared_request_state>;
                    data_type m_ptr;

                    request2() = default;
                    
                    request2(const data_type& ptr) : m_ptr{ptr} {}
                    request2(data_type&& ptr) : m_ptr{std::move(ptr)} {}
                    request2(request2&&) = default;
                    request2& operator=(request2&&) = default;
                    request2(const request2&) = delete;
                    request2& operator=(const request2&) = delete;
                    
                    void wait();
                    bool test();
                    bool ready();
                };
            
            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_REQUEST_STATE_HPP */

