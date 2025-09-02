/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef LINEAIRDB_THREAD_KEY_STORAGE_H
#define LINEAIRDB_THREAD_KEY_STORAGE_H

#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <atomic>
#include <functional>
#include <iostream>
#include <mutex>

template <class T>
class ThreadKeyStorage {
  struct TlsNode {
    TlsNode* prev;
    T payload;
    TlsNode() : prev(nullptr), payload() {}
    template <class... Ts>
    TlsNode(Ts... args) : prev(nullptr), payload(&args...) {}
    template <class U>
    TlsNode(std::function<U()> f) : prev(nullptr), payload(std::move(f())) {}
  };

 public:
  ThreadKeyStorage(int = 0) : head_node_(nullptr) {
    int err = ::pthread_key_create(&key_, nullptr);
    if (err != 0) {
      std::cerr << "::pthread_key_create failed: " << err << std::endl;
    }
  }

  ~ThreadKeyStorage() {
    auto* ptr = head_node_.load();
    while (ptr != nullptr) {
      auto* prev = ptr->prev;
      delete ptr;
      ptr = prev;
    }
    int err = ::pthread_key_delete(key_);
    if (err != 0) {
      std::cerr << "::pthread_key_delete failed: " << err << std::endl;
    }
  }

  /**
   * @brief
   * Get thread-local objects with initializer function.
   *
   * @tparam U
   * @param func
   * @return T*
   */
  template <class U>
  T* Get(std::function<U()>&& func) {
    void* ptr = pthread_getspecific(key_);
    if (ptr == nullptr) {
      TlsNode* new_obj = new TlsNode(std::move(func));
      int err = ::pthread_setspecific(key_, new_obj);
      if (err == ENOMEM) {
        std::cerr << "::pthread_setspecific failed: no enough memory"
                  << std::endl;
        exit(EXIT_FAILURE);
      } else if (err == EINVAL) {
        std::cerr << "::pthread_setspecific failed: invalid key" << std::endl;
        exit(EXIT_FAILURE);
      }
      for (;;) {
        TlsNode* old = head_node_.load();
        new_obj->prev = old;
        bool ret = head_node_.compare_exchange_weak(old, new_obj);
        if (ret) {
          break;
        }
      }
      ptr = new_obj;
    }
    return &reinterpret_cast<TlsNode*>(ptr)->payload;
  }

  T* Get() {
    void* ptr = pthread_getspecific(key_);
    if (ptr == nullptr) {
      TlsNode* new_obj = new TlsNode();
      int err = ::pthread_setspecific(key_, new_obj);
      if (err == ENOMEM) {
        std::cerr << "::pthread_setspecific failed: no enough memory"
                  << std::endl;
        exit(EXIT_FAILURE);
      } else if (err == EINVAL) {
        std::cerr << "::pthread_setspecific failed: invalid key" << std::endl;
        exit(EXIT_FAILURE);
      }
      for (;;) {
        TlsNode* old = head_node_.load();
        new_obj->prev = old;
        bool ret = head_node_.compare_exchange_weak(old, new_obj);
        if (ret) {
          break;
        }
      }
      ptr = new_obj;
    }
    return &reinterpret_cast<TlsNode*>(ptr)->payload;
  }

  void ForEach(std::function<void(T*)>&& f) {
    TlsNode* ptr = head_node_.load();
    while (ptr != nullptr) {
      f(&ptr->payload);
      ptr = ptr->prev;
    }
  }

  void Every(std::function<bool(T*)>&& f) {
    TlsNode* ptr = head_node_.load();
    while (ptr != nullptr) {
      auto result = f(&ptr->payload);
      if (!result) break;
      ptr = ptr->prev;
    }
  }

 private:
  pthread_key_t key_;
  std::atomic<TlsNode*> head_node_;
};

#endif  // LINEAIRDB_THREAD_KEY_STORAGE_H
