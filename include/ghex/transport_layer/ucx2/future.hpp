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
#ifndef INCLUDED_GHEX_TL_UCX_FUTURE_HPP
#define INCLUDED_GHEX_TL_UCX_FUTURE_HPP

#include "./request.hpp"

namespace gridtools{
    namespace ghex {
        namespace tl {
            namespace ucx {

                template<typename T>
                struct future_ref
                {
                    using value_type  = T;
                    using handle_type = request;

                    value_type* m_data = nullptr;
                    handle_type m_handle;

                    future_ref() = default;

                    future_ref(value_type& data, handle_type&& h) 
                    :   m_data(&data)
                    ,   m_handle(std::move(h))
                    {}
                    future_ref(const future_ref&) = delete;
                    future_ref(future_ref&&) = default;
                    future_ref& operator=(const future_ref&) = delete;
                    future_ref& operator=(future_ref&&) = default;

                    void wait() noexcept
                    {
                        m_handle.wait();
                    }

                    bool test() noexcept
                    {
                        return m_handle.test();
                    }

                    bool ready() noexcept
                    {
                        return m_handle.test();
                    }

                    [[nodiscard]] value_type& get()
                    {
                        wait(); 
                        return *m_data; 
                    }

                    /** Cancel the future.
                      * @return True if the request was successfully canceled */
                    bool cancel()
                    {
                        return true;
                    }
                };

                /** @brief future template for non-blocking communication */
                template<typename T>
                struct future
                {
                    using value_type  = T;
                    using handle_type = request;

                    value_type m_data;
                    handle_type m_handle;

                    future(value_type&& data, handle_type&& h) 
                    :   m_data(std::move(data))
                    ,   m_handle(std::move(h))
                    {}
                    future(const future&) = delete;
                    future(future&&) = default;
                    future& operator=(const future&) = delete;
                    future& operator=(future&&) = default;

                    void wait() noexcept
                    {
                        m_handle.wait();
                    }

                    bool test() noexcept
                    {
                        return m_handle.test();
                    }

                    bool ready() noexcept
                    {
                        return m_handle.test();
                    }

                    [[nodiscard]] value_type get()
                    {
                        wait(); 
                        return std::move(m_data); 
                    }

                    /** Cancel the future.
                      * @return True if the request was successfully canceled */
                    bool cancel()
                    {
                        return true;
                    }
                };

                template<>
                struct future<void>
                {
                    using handle_type = request;

                    handle_type m_handle;

                    future() noexcept = default; 
                    future(handle_type&& h) 
                    :   m_handle(std::move(h))
                    {}
                    future(const future&) = delete;
                    future(future&&) = default;
                    future& operator=(const future&) = delete;
                    future& operator=(future&&) = default;

                    void wait() noexcept
                    {
                        m_handle.wait();
                    }

                    bool test() noexcept
                    {
                        return m_handle.test();
                    }

                    bool ready() noexcept
                    {
                        return m_handle.test();
                    }

                    void get()
                    {
                        wait(); 
                    }

                    bool cancel()
                    {
                        return true;
                    }
                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_FUTURE_HPP */

