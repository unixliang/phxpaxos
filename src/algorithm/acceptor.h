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

#include "base.h"
#include <string>
#include "comm_include.h"
#include "paxos_log.h"
#include "soft_state.h"

namespace phxpaxos
{

class AcceptorState
{
public:
  AcceptorState(const Config * poConfig, const LogStorage * poLogStorage, SoftState *poSoftState);
    ~AcceptorState();

    void Init();

    const BallotNumber & GetAcceptedBallot() const;
    void SetAcceptedBallot(const BallotNumber & oAcceptedBallot);

    const std::string & GetAcceptedValue();
    void SetAcceptedValue(const std::string & sAcceptedValue);

    const uint32_t GetChecksum() const;

    int Persist(const uint64_t llInstanceID, const BallotNumber & oPromiseBallot);

//private:
    BallotNumber m_oAcceptedBallot;
    std::string m_sAcceptedValue;

    Config * m_poConfig;
    PaxosLog m_oPaxosLog;

    int m_iSyncTimes;

  SoftState *m_poSoftState;
};

////////////////////////////////////////////////////////////////


class Group;
class Instance;

class Acceptor : public Base
{
public:
    Acceptor(
            const Config * poConfig, 
            const MsgTransport * poMsgTransport, 
            const LogStorage * poLogStorage,
            Group * poGroup);
    ~Acceptor();

    void Init(uint64_t llInstanceID);

    AcceptorState * GetAcceptorState();

    int OnPrepare(const PaxosMsg & oPaxosMsg);

    void OnAccept(const PaxosMsg & oPaxosMsg);

private:
    AcceptorState m_oAcceptorState;

    Group * m_poGroup;

};
    
}
