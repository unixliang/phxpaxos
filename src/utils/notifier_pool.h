/*
Tencent is pleased to support the open source community by making 
PhxPaxos available.
Copyright (C) 2016 THL A29 Limited, a Tencent company. 
All rights reserved.

Licensed under the BSD 3-Clause License (the "License"); you may 
not use this file except in compliance with the License. You may 
obtain a copy of the License at

https://opensource.org/licenses/BSD-3-Clause

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" basis, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
implied. See the License for the specific language governing 
permissions and limitations under the License.

See the AUTHORS file for names of contributors. 
*/

#pragma once

#include <map>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <queue>
#include <type_traits>

namespace phxpaxos
{

class Notifier
{
public:
    Notifier();
    ~Notifier();

    int Init();

    void SendNotify(const int ret);

    void WaitNotify(int & ret);

private:
    int m_iPipeFD[2];
};

/////////////////////////////////

class NotifierPool
{
public:
    NotifierPool();
    ~NotifierPool();

    int GetNotifier(const uint64_t iID, Notifier *& poNotifier);

private:
    std::map<uint64_t, Notifier *> m_mapPool;
    std::mutex m_oMutex;
};

template <typename T> class ConcurrentQueue {
public:
  void push(const T &Val) {
    {
      std::lock_guard<std::mutex> Lock(M);
      ++Counter;
      Q.push(Val);
    }
    Cv.notify_one();
  }
  void push(T &&Val) {
    {
      std::lock_guard<std::mutex> Lock(M);
      ++Counter;
      Q.emplace(Val);
    }
    Cv.notify_one();
  }
  // Block if the queue is empty
  T pop(bool &ok) {
    std::unique_lock<std::mutex> Lock(M);
    Cv.wait(Lock, [this] { return Counter > 0 || Shutdown == true; });
    if (Counter > 0) {
      ok = true;
      --Counter;
      T Front = Q.front();
      Q.pop();
      Lock.unlock();
      return Front;
    }
    ok = false;
    Lock.unlock();
    return T();
  }
  void shutdown() {
    {
      std::lock_guard<std::mutex> Lock(M);
      Shutdown = true;
    }
    Cv.notify_all();
  }

  ConcurrentQueue() = default;

private:
  uint64_t Counter = 0;
  std::mutex M;
  std::condition_variable Cv;
  std::queue<T> Q;
  bool Shutdown = false;
};

}
