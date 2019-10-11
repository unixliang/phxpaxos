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

#include <vector>
#include <inttypes.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <functional>
#include <thread>
#include <iostream>
#include <poll.h>
#include <sys/timerfd.h>
#include <unistd.h>

namespace phxpaxos
{

class Timer
{
public:
    Timer();
    ~Timer();

    void AddTimer(const uint64_t llAbsTime, uint32_t & iTimerID);
    
    void AddTimerWithType(const uint64_t llAbsTime, const int iType, uint32_t & iTimerID);

    bool PopTimeout(uint32_t & iTimerID, int & iType);

    const int GetNextTimeout() const;
    
private:
    struct TimerObj
    {
        TimerObj(uint32_t iTimerID, uint64_t llAbsTime, int iType) 
            : m_iTimerID(iTimerID), m_llAbsTime(llAbsTime), m_iType(iType) {}

        uint32_t m_iTimerID;
        uint64_t m_llAbsTime;
        int m_iType;

        bool operator < (const TimerObj & obj) const
        {
            if (obj.m_llAbsTime == m_llAbsTime)
            {
                return obj.m_iTimerID < m_iTimerID;
            }
            else
            {
                return obj.m_llAbsTime < m_llAbsTime;
            }
        }
    };

private:
    uint32_t m_iNowTimerID;
    std::vector<TimerObj> m_vecTimerHeap;
};

class Alarm {
public:
  Alarm(const uint64_t strictMs, std::function<void()> Func_)
      : Func(Func_), Timeout(strictMs) {
    Timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if(Timerfd == -1)
      abort();
    int res = pipe(Pipes.data());
    if(res == -1)
      abort();
    setDuration();
    disarm();
    T = std::move(std::thread([this]() {
      std::array<struct pollfd, 2> pfds;
      pfds[0].fd = Timerfd;
      pfds[0].events = POLLIN;
      pfds[1].fd = Pipes[0];
      pfds[1].events = POLLIN;
      uint64_t Exp;
      while (true) {
        auto Npfds = pfds;
        poll(Npfds.data(), 2, -1);
        if (Npfds[1].revents & POLLIN)
          break;
        ssize_t ress = 0;
        if (Npfds[0].revents & POLLIN) {
          ress = read(Timerfd, &Exp, sizeof(uint64_t));
          if (ress == -1 && errno != EAGAIN)
            abort();
        }
        Func();
      }
    }));
  }

  void rearm() {
    Armed = true;
    setDuration();
    struct itimerspec new_value;
    new_value.it_value = new_value.it_interval = Duration;
    if (timerfd_settime(Timerfd, 0, &new_value, nullptr) == -1) {
      // LOG(FATAL) << errno << "rearm() failed\n";
      abort(); // Cannot execute Raft
    }
  }
  void disarm() {
    Armed = false;
    setDuration();
    struct itimerspec new_value;
    new_value.it_value.tv_nsec = 0;
    new_value.it_value.tv_sec = 0;
    new_value.it_interval = Duration;
    if (timerfd_settime(Timerfd, 0, &new_value, nullptr) == -1) {
      // LOG(FATAL) << errno << "disarm() failed\n";
      abort(); // Cannot execute Raft
    }
  }
  uint64_t getElapsedTimeinMs() {
    struct itimerspec curr_value;
    timerfd_gettime(Timerfd, &curr_value);
    curr_value.it_interval.tv_sec -= curr_value.it_value.tv_sec;
    if (curr_value.it_interval.tv_nsec >= curr_value.it_value.tv_nsec)
      curr_value.it_interval.tv_nsec -= curr_value.it_value.tv_nsec;
    else {
      --curr_value.it_interval.tv_sec;
      curr_value.it_interval.tv_nsec +=
          1000000000 - static_cast<uint64_t>(curr_value.it_value.tv_nsec);
    }
    return curr_value.it_interval.tv_sec * 1000 +
           curr_value.it_interval.tv_nsec / 1000000;
  }

  ~Alarm() {
    ssize_t res = write(Pipes[1], "$EOF", 4);
    if(res==-1)
        abort();
    //CHECK_NE(res, -1) << errno;
    T.join();
    close(Pipes[0]);
    close(Pipes[1]);
    close(Timerfd);
  }

  bool isArmed() { return Armed; }

  void resetTimeout(const uint64_t T) {
    Timeout = T;
    if(isArmed())
      rearm();
  }

private:
  void setDuration() {
    Duration.tv_sec = Timeout / 1000;
    Duration.tv_nsec = (Timeout % 1000) * 1000000;
  }
  int Timerfd;
  struct timespec Duration;
  std::function<void()> Func;
  std::thread T;
  bool Armed = false;
  std::array<int, 2> Pipes;
  uint64_t Timeout;
};

}
