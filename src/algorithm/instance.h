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
#include "acceptor.h"
#include "proposer.h"
#include "group.h"
#include "msg_transport.h"
#include "phxpaxos/sm.h"
#include "sm_base.h"
#include "phxpaxos/storage.h"
#include "comm_include.h"
#include "ioloop.h"
#include "commitctx.h"
#include "committer.h"
#include "cp_mgr.h"

namespace phxpaxos
{

class Instance
{
public:
    Instance(
            const Config * poConfig, 
            const LogStorage * poLogStorage,
            const MsgTransport * poMsgTransport,
            const Options & oOptions,
            Group * poGroup);
    ~Instance();

    int Init(uint64_t llNowInstanceID);

public:
    Acceptor * GetAcceptor();

    void SetCommitCtx(std::shared_ptr<CommitCtx> poCommitCtx);

    std::shared_ptr<CommitCtx> GetCommitCtx() const;

public:
    void OnNewValueCommitTimeout();


public:
    int OnReceivePaxosMsg(const PaxosMsg & oPaxosMsg, const bool bIsRetry = false);
    
    int ReceiveMsgForProposer(const PaxosMsg & oPaxosMsg);
    
    int ReceiveMsgForAcceptor(const PaxosMsg & oPaxosMsg, const bool bIsRetry);
    
    int ReceiveMsgForLearner(const PaxosMsg & oPaxosMsg);

public:
    void OnTimeout(const uint32_t iTimerID, const int iType);

public:
/*
    bool SMExecute(
        const uint64_t llInstanceID, 
        const std::string & sValue, 
        SMCtx * poSMCtx);
*/
public:
    int NewValue(const std::string & sValue);

private:
    void ChecksumLogic(const PaxosMsg & oPaxosMsg);

    int ProtectionLogic_IsCheckpointInstanceIDCorrect(const uint64_t llCPInstanceID, const uint64_t llLogMaxInstanceID);

private:
    Config * m_poConfig;
    MsgTransport * m_poMsgTransport;

    Acceptor m_oAcceptor;
    Proposer m_oProposer;

    PaxosLog m_oPaxosLog;

private:
    std::shared_ptr<CommitCtx> m_poCommitCtx;
    uint32_t m_iCommitTimerID;

    uint64_t m_llInstanceID;


private:


private:
    TimeStat m_oTimeStat;
    Options m_oOptions;

    Group * m_poGroup;
};
    
}
