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
#include <function>
#include <inttypes.h>

namespace phxpaxos
{

class Timer
{
public:
    using CallbackFunc = std::function<void(const uint32_t /*iTimerID*/)>;

    Timer();
    ~Timer();

    void AddTimer(const uint64_t llAbsTime, uint32_t & iTimerID);
    
    void AddTimerWithCallbackFunc(const uint64_t llAbsTime, CallbackFunc fCallbackFunc, uint32_t & iTimerID);

    bool PopTimeout(uint32_t & iTimerID, uint64_t & llInstanceID, CallbackFunc & fCallbackFunc);

    const int GetNextTimeout() const;
    
private:
    struct TimerObj
    {
        TimerObj(uint32_t iTimerID, uint64_t llAbsTime, CallbackFunc fCallbackFunc) 
            : m_iTimerID(iTimerID), m_llAbsTime(llAbsTime), m_fCallbackFunc(fCallbackFunc) {}

        uint32_t m_iTimerID;
        uint64_t m_llAbsTime;
        CallbackFunc m_fCallbackFunc;

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
    
}
