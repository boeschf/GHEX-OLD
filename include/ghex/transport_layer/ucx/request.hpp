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

#include "./error.hpp"

namespace gridtools{
    namespace ghex {
        namespace tl {
            namespace ucx {

                struct request
                {
                    ucp_worker_h m_worker;
                    void*        m_ucx_ptr;
                    bool         m_ready = false;

                    void wait()
                    {
                        while (!test()) {}
                    }

                    bool test()
                    {
                        if (m_ready) return true;
                        ucp_worker_progress(m_worker);
                        m_ready = (ucp_request_check_status(m_ucx_ptr) == UCS_OK);
                        return m_ready;
                    }
                };

                template<typename Allocator>
                struct internal_request
                {
                    static constexpr std::size_t s_padding = sizeof(request)+alignof(request)-1;
                    static constexpr std::size_t s_mask    = ~std::size_t{alignof(request)-1};

                    unsigned char* m_data = nullptr;
                    request*       m_request = nullptr;
                    std::size_t    m_size;
                    Allocator      m_alloc;

                    ~internal_request()
                    {
                        if (m_data)
                            m_alloc.deallocate(m_data, m_size);
                    }

                    internal_request(std::size_t size_, ucp_worker_h worker_, Allocator alloc_)
                    : m_data{
                        size_>0u?
                            alloc_.allocate(size_+s_padding) :
                            nullptr }
                    , m_request{
                        ::new( (void*)reinterpret_cast<unsigned char*>(
                            (reinterpret_cast<std::uintptr_t>(m_data + size_) + alignof(request) - 1) & s_mask) )
                        request{worker_, m_data + size_}}
                    , m_size{ size_+ s_padding }
                    , m_alloc{ alloc_ }
                    {}

                    internal_request(const internal_request&) = delete;

                    internal_request(internal_request&& other) noexcept
                    : m_data{ other.m_data }
                    , m_request{ other.m_request }
                    , m_size{ other.m_size }
                    , m_alloc{ std::move(other.m_alloc) }
                    {
                        other.m_data = nullptr;
                        other.m_request = nullptr;
                    }

                    void wait()
                    {
                        m_request->wait();
                    }

                    bool test()
                    {
                        return m_request->test();
                    }

                    void* ucx_request_ptr()
                    {
                        return m_request->m_ucx_ptr;
                    }
                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_REQUEST_HPP */

