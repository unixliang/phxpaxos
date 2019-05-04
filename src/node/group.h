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

#include <thread>
#include <vector>
#include "comm_include.h"
#include "config_include.h"
#include "instance.h"
#include "cleaner.h"
#include "communicate.h"
#include "phxpaxos/options.h"
#include "phxpaxos/network.h"

namespace phxpaxos
{

class Group
{
public:
    Group(LogStorage * poLogStorage, 
            NetWork * poNetWork,    
            InsideSM * poMasterSM,
            const int iGroupIdx,
            const Options & oOptions);

    ~Group();

    void StartInit();

    void InitLastCheckSum();

    void Init();

    int GetInitRet();

    void Start();

    void Stop();

    Config * GetConfig();

    Instance * GetInstance(uint64_t llInstanceID);

    Committer * GetCommitter();

    Cleaner * GetCheckpointCleaner();

    Replayer * GetCheckpointReplayer();

    void AddStateMachine(StateMachine * poSM);

    uint64_t GetProposalID() const;

    void NewPrepare();

    void SetOtherProposalID(const uint64_t llOtherProposalID);

    uint32_t GetMaxWindowSize();

    void SetPromiseBallot(const uint64_t llInstanceID, const BallotNumber &oBallotNumber);

    BallotNumber GetPromiseBallot(const uint64_t llInstanceID, uint64_t & llEndPromiseInstanceID) const;

    void OnReceiveCheckpointMsg(const CheckpointMsg & oCheckpointMsg);

    bool ReceiveMsgHeaderCheck(const Header & oHeader, const nodeid_t iFromNodeID);

    void OnReceive(const std::string & sBuffer);

    void ReceiveMsgForLearner(const PaxosMsg & oPaxosMsg);


    void SetPromiseInfo(const uint64_t llPromiseInstanceID, const uint64_t llEndPromiseInstanceID);

    bool NeedPrepare(const uint64_t llInstanceID);

    Learner * GetLearner();

    bool HasIdleInstance(uint64_t & llInstanceID);

    void AddTimeoutInstance(const uint64_t llInstaceID);
    bool HasTimeoutInstance(uint64_t & llInstanceID);

    int NewValue(const uint64_t llInstanceID, const std::string & sValue);

    void NewIdleInstance();

    //this funciton only enqueue, do nothing.
    int OnReceiveMessage(const char * pcMessage, const int iMessageLen);

    IOLoop * GetIOLoop() const;

private:
    int GetMaxInstanceIDFromLog(uint64_t & llMaxInstanceID);

    int ProtectionLogic_IsCheckpointInstanceIDCorrect(const uint64_t llCPInstanceID, const uint64_t llLogMaxInstanceID);

    int PlayLog(const uint64_t llBeginInstanceID, const uint64_t llEndInstanceID);

private:
    LogStorage * m_poLogStorage;

    Communicate m_oCommunicate;
    Config m_oConfig;
    std::map<uint64_t, Instance> m_mapInstances;
    uint32_t m_iMaxWindowSize{100};

    int m_iInitRet{0};
    std::thread * m_poThread;

    CheckpointMgr m_oCheckpointMgr;
    Learner m_oLearner;
    SMFac m_oSMFac;

    uint64_t m_llNowInstanceID{-1};
    uint64_t m_llNowIdleInstanceID{-1};
    uint32_t m_iLastChecksum{0};

    uint64_t m_llProposalID{0}; // for proposer Prepare/Accept
    uint64_t m_llHighestOtherProposalID{0};

    std::set<uint64_t> m_setPromiseInstanceID;
    std::set<uint64_t> m_setEndPromiseInstanceID;
    uint64_t m_llEndPromiseInstanceID{-1};

    std::map<uint64_t, BallotNumber> m_mapInstanceID2PromiseBallot; // for acceptor OnPrepare

    Committer m_oCommitter;

    bool m_bStarted{false};

    IOLoop m_oIOLoop;

    std::set<uint64_t> m_seTimeoutInstnaceList;
};
    
}
