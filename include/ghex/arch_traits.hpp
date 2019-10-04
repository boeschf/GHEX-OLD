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
#ifndef INCLUDED_GHEX_ARCH_TRAITS_HPP
#define INCLUDED_GHEX_ARCH_TRAITS_HPP

#include "./allocator/aligned_allocator_adaptor.hpp"
#include "./allocator/cuda_allocator.hpp"
#include "./transport_layer/message_buffer.hpp"
#include "./arch_list.hpp"

namespace gridtools {
    namespace ghex {

        template<typename Arch>
        struct arch_traits;

        template<>
        struct arch_traits<cpu>
        {
            static constexpr const char* name = "CPU";

            using device_id_type          = int;
            using message_allocator_type  = allocator::aligned_allocator_adaptor<std::allocator<unsigned char>,64>;
            using message_type            = tl::message_buffer<message_allocator_type>;

            static device_id_type default_id() { return 0; }

            static message_type make_message(device_id_type index = default_id()) 
            { 
                static_assert(std::is_same<decltype(index),device_id_type>::value, "trick to prevent warnings");
                return {};
            }
        };

#ifdef __CUDACC__
                template<typename T>
                struct vector_type_
                {
                    T* m_data = nullptr;
                    std::size_t m_size = 0;
                    std::size_t m_capacity = 0;

                    const T* data() const { return m_data; }
                    T* data() { return m_data; }
                    std::size_t size() const { return m_size; }
                    std::size_t capacity() const { return m_capacity; }

                    vector_type_() = default;
                    vector_type_(const vector_type_&) = delete;
                    vector_type_(vector_type_&& other)
                    : m_data(other.m_data), m_size(other.m_size), m_capacity(other.m_capacity)
                    {
                        other.m_size = 0u;
                        other.m_capacity = 0u;
                    }

                    void resize(std::size_t new_size)
                    {
                        if (new_size <= m_capacity)
                        {
                            m_size = new_size;
                        }
                        else
                        {
                            // not freeing because of CRAY-BUG
                            //cudaFree(m_data);
                            std::size_t new_capacity = std::max(new_size, (std::size_t)(m_capacity*1.6));
                            cudaMalloc((void**)&m_data, new_capacity*sizeof(T));
                            m_capacity = new_capacity;
                            m_size = new_size;
                        }
                    }

                    ~vector_type_()
                    {
                        if (m_capacity > 0u)
                        {
                            // not freeing because of CRAY-BUG
                            //cudaFree(m_data);
                        }
                    }
                };
        template<>
        struct arch_traits<gpu>
        {
            static constexpr const char* name = "GPU";

            using device_id_type          = int;
            using message_allocator_type  = allocator::cuda::allocator<unsigned char>;
            //using message_type            = tl::message_buffer<message_allocator_type>;
            using message_type = vector_type_<unsigned char>;

            static device_id_type default_id() { return 0; }

            static message_type make_message(device_id_type index = default_id()) 
            { 
                static_assert(std::is_same<decltype(index),device_id_type>::value, "trick to prevent warnings");
                return {};
            }
        };
#else
#ifdef GHEX_EMULATE_GPU
        template<>
        struct arch_traits<gpu> : public arch_traits<cpu> {};
#endif
#endif

    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_ARCH_TRAITS_HPP */

