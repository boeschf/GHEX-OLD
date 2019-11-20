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
#include <iostream>

namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace ucx {

                struct request
                {
                    void*     m_ptr = nullptr;
                    worker_t* m_worker =  nullptr;
                    worker_t* m_other_worker =  nullptr;

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
                        //return test();
                        progress_only();
                        return test_only();
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
                        //std::cout << "test only" << std::endl;
                        if (m_ptr)
                        {
                            //m_worker->lock();
                            const auto res = (ucp_request_check_status(m_ptr) != UCS_INPROGRESS);
                            //m_worker->unlock();
                            //std::cout << "test only done" << std::endl;
                            return res;
                        }
                        else
                        {
                            //std::cout << "test only done" << std::endl;
                            return true;
                        }
                    }

                    void progress_only()
                    {
                        //std::cout << "progress only" << std::endl;
                        if (m_worker)
                        {
                            //m_worker->lock();
                            //if (!m_worker->m_shared)
                            //{
                            //    const auto res = m_worker->m_lock->m_locked.load();
                            //    if (res != true)
                            //    {
                            //        std::cout << "WTFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF" << std::endl;
                            //        std::terminate();
                            //    }
                            //    //std::cout << "ucp progress begin" << std::endl;
                                ucp_worker_progress(m_worker->get());
                            //    //std::cout << "ucp progress end" << std::endl;
                            //}
                            //else
                            //    ucp_worker_progress(m_worker->get());
                            //m_worker->unlock();
                        }
                        //std::cout << "progress only done" << std::endl;
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
                            if (ucp_request_check_status(m_ptr) != UCS_INPROGRESS)
                            {
                                //ucp_worker_progress(m_other_worker->get());
                                return true;
                            }
                            m_worker->progress(m_other_worker);
                            return (ucp_request_check_status(m_ptr) != UCS_INPROGRESS);
                        }
                        else
                        {
                            //ucp_worker_progress(m_worker->get());
                            //ucp_worker_progress(m_other_worker->get());
                            return true;
                        }
                    }
                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_REQUEST_HPP */

