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
#ifndef INCLUDED_GHEX_TL_UCX_REQUEST_HPP
#define INCLUDED_GHEX_TL_UCX_REQUEST_HPP

#include "./worker.hpp"

namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace ucx {

                struct request
                {
                    void*     m_ptr = nullptr;
                    worker_t* m_worker;
                    worker_t* m_other_worker;

                    request() noexcept = default;
                    
                    request(void* ptr, worker_t* worker, worker_t* other_worker) noexcept
                    : m_ptr(ptr)
                    , m_worker(worker)
                    , m_other_worker(other_worker)
                    {}

                    request(request&& other) noexcept
                    : m_ptr(other.m_ptr)
                    , m_worker(other.m_worker)
                    , m_other_worker(other.m_other_worker)
                    {
                        other.m_ptr = nullptr;
                    }

                    request& operator=(request&& other) noexcept
                    {
                        if (m_ptr)
                            ucp_request_free(m_ptr);
                        m_ptr = other.m_ptr;
                        m_worker = other.m_worker;
                        m_other_worker = other.m_other_worker;
                        other.m_ptr = nullptr;
                        return *this;
                    }

                    request(const request&) = delete;
                    request& operator=(const request&) = delete;
                    
                    ~request() noexcept
                    {
                        if (m_ptr)
                            ucp_request_free(m_ptr);
                    }

                    bool ready()
                    {
                        //return test_this();
                        return test();
                    }

                    /*bool test_this()
                    {
                        if (m_ptr)
                        {
                            progress_this();
                            return (ucp_request_check_status(m_ptr) != UCS_INPROGRESS);
                        }
                        else
                            return true;
                    }*/
                    
                    bool test_only()
                    {
                        if (m_ptr)
                            return (ucp_request_check_status(m_ptr) != UCS_INPROGRESS);
                        else
                            return true;
                    }
                    
                    /*void progress_this()
                    {
                        ucp_worker_progress(m_worker->get());
                    }*/

                    // expensive from here
                    void wait()
                    {
                        while(!test()) {}
                    }

                    bool test()
                    {
                        if (m_ptr)
                        {
                            m_worker->progress(m_other_worker);
                            return (ucp_request_check_status(m_ptr) != UCS_INPROGRESS);
                        }
                        else
                            return true;
                    }
                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_REQUEST_HPP */

