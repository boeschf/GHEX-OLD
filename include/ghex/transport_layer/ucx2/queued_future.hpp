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
#ifndef INCLUDED_GHEX_TL_UCX_QUEUED_FUTURE_HPP
#define INCLUDED_GHEX_TL_UCX_QUEUED_FUTURE_HPP

#include "./future.hpp"

namespace gridtools{
    namespace ghex {
        namespace tl {
            namespace ucx {

                struct queued_future_state
                {
                    virtual bool& ready() noexcept = 0;
                    virtual void call_continuation() = 0;
                    virtual bool test_only() = 0;
                    virtual void progress() = 0;
                    //virtual ~queued_future_state() noexcept {}
                };

                template<typename T>
                struct queued_future
                {
                    using future_type = future_ref<T>;
                    using value_type  = typename future_type::value_type;
                    using handle_type = typename future_type::handle_type;

                    struct data_type : final public queued_future_state
                    {
                        future_type m_future;
                        std::function<void(value_type&)> m_continuation;
                        bool m_has_continuation = false;
                        bool m_ready = false;
                        bool m_called = false;

                        bool& ready() noexcept override { return m_ready; }
                        void call_continuation() override 
                        { 
                            if (m_has_continuation)
                            {
                                m_called = true;
                                m_continuation( *(m_future.m_data) );
                            }
                        }
                        bool test_only() override { m_future.m_handle.test_only(); }
                        void progress() override { m_future.m_hanlde.progress(); }
                    };

                    std::unique_ptr<data_type> m_data;

                    queued_future(future_type&& fut)
                    : std::make_unique<data_type>({std::move(fut)})
                    {}

                    queued_future(const queued_future&) = delete;
                    queued_future(queued_future&&) = default;
                    queued_future& operator=(const queued_future&) = delete;
                    queued_future& operator=(queued_future&&) = default;

                    template<typename Func>
                    queued_future& then(Func&& func)
                    {
                        m_data->m_continuation = std::forward<Func>(func);
                        m_data->m_has_continuation = true;
                        return *this;
                    }

                    void wait()
                    {
                        while(!test()) {}
                        if (m_data->m_continuation && !m_data->m_called)
                            //m_data->m_future.m_data = std::move( m_data->m_continuation( std::move(m_data->m_future.m_data) ) );
                            m_data->m_continuation( *(m_data->m_future.m_data) );
                    }

                    bool test() noexcept const
                    {
                        return ready();
                    }

                    bool ready() noexcept const
                    {
                        return m_data->m_ready;
                    }

                    [[nodiscard]] value_type& get()
                    {
                        wait();
                        return *(m_data->m_future.m_data);
                    }

                    queued_future_state* state() { return m_data.get(); }

                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_QUEUED_FUTURE_HPP */

