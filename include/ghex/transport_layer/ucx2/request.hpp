#ifndef INCLUDED_GHEX_TL_UCX_REQUEST_HPP
#define INCLUDED_GHEX_TL_UCX_REQUEST_HPP

#include "./error.hpp"

namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace ucx {

                struct request
                {
                    void* m_ptr = nullptr;
                    ucp_worker_h m_worker;

                    request() noexcept = default;
                    
                    request(void* ptr, ucp_worker_h worker) noexcept
                    : m_ptr(ptr)
                    , m_worker(worker)
                    {}

                    request(request&& other) noexcept
                    : m_ptr(other.m_ptr)
                    , m_worker(other.m_worker)
                    {
                        other.m_ptr = nullptr;
                    }

                    request& operator=(request&& other) noexcept
                    {
                        if (m_ptr)
                            ucp_request_free(m_ptr);
                        m_ptr = other.m_ptr;
                        m_worker = other.m_worker;
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

                    void wait()
                    {
                        while(!test()) {}
                    }

                    bool test()
                    {
                        if (m_ptr)
                        {
                            progress();
                            return (ucp_request_check_status(m_ptr) != UCS_INPROGRESS);
                        }
                        else
                            return true;
                    }

                    void progress()
                    {
                        ucp_worker_progress(m_worker);
                    }
                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_REQUEST_HPP */

