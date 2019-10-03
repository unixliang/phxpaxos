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
#include "committer.h"
#include "learner.h"
#include "cp_mgr.h"
#include "phxpaxos/options.h"
#include "phxpaxos/network.h"
#include "soft_state.h"

namespace phxpaxos
{

class Group
{
public:
    Group(LogStorage * poLogStorage, 
            NetWork * poNetWork,    
            InsideSM * poMasterSM,
            SoftState * poSoftState,
            const int iGroupIdx,
            const Options & oOptions);

    ~Group();

    void StartInit();

    int LoadMaxInstanceID(uint64_t & llInstanceID);

    int InitLastCheckSum();

    void Init();

    int GetInitRet();

    void Start();

    void Stop();

    Config * GetConfig();

    Instance * GetInstance(uint64_t llInstanceID);

    Committer * GetCommitter();

    Cleaner * GetCheckpointCleaner();

    Replayer * GetCheckpointReplayer();

    void OnReceiveCheckpointMsg(const CheckpointMsg & oCheckpointMsg);

    bool ReceiveMsgHeaderCheck(const Header & oHeader, const nodeid_t iFromNodeID);

    void OnReceive(const std::string & sBuffer);

    int OnReceivePaxosMsg(const PaxosMsg & oPaxosMsg, const bool bIsRetry = false);

    void ReceiveMsgForLearner(const PaxosMsg & oPaxosMsg);

    void ProcessCommit();

    Learner * GetLearner();

    SMFac * GetSMFac();

    SoftState * GetSoftState();

    bool HasIdleInstance(uint64_t & llInstanceID);

    void AddTimeoutInstance(const uint64_t llInstaceID);
    bool HasTimeoutInstance(uint64_t & llInstanceID);

    int NewValue(const uint64_t llInstanceID, const std::string & sValue, std::shared_ptr<CommitCtx> poCommitCtx);

    void NewIdleInstance();

    //this funciton only enqueue, do nothing.
    int OnReceiveMessage(const char * pcMessage, const int iMessageLen);

    IOLoop * GetIOLoop();

    const Options * GetOptions();

    uint64_t GetNowInstanceID() const;

    uint64_t GetMinChosenInstanceID() const;

    int GetInstanceValue(const uint64_t llInstanceID, std::string & sValue, int & iSMID);

    void AddStateMachine(StateMachine * poSM);

private:
    int GetMaxInstanceIDFromLog(uint64_t & llMaxInstanceID);

    int ProtectionLogic_IsCheckpointInstanceIDCorrect(const uint64_t llCPInstanceID, const uint64_t llLogMaxInstanceID);

    int RebuildInstance(const uint64_t llBeginInstanceID, const uint64_t llEndInstanceID);

    int PlayLog(const uint64_t llBeginInstanceID, const uint64_t llEndInstanceID);

    int RebuildSoftState(const uint64_t llMinChosenInstanceID, const uint64_t llMaxInstanceID);

private:
    Options m_oOptions;
    LogStorage * m_poLogStorage;
    PaxosLog m_oPaxosLog;

    int m_iMyGroupIdx;
 
    Config m_oConfig;
    Communicate m_oCommunicate;
    std::map<uint64_t, std::unique_ptr<Instance>> m_mapInstances;
    uint32_t m_iMaxWindowSize{100};


    uint64_t m_llNowInstanceID{-1};
    uint64_t m_llNowIdleInstanceID{-1};

    bool m_bStarted{false};

    IOLoop m_oIOLoop;

    SMFac m_oSMFac;

    Committer m_oCommitter;

    CheckpointMgr m_oCheckpointMgr;

    Learner m_oLearner;

    int m_iInitRet{0};
    std::thread * m_poThread;

    std::set<uint64_t> m_seTimeoutInstnaceList;

  SoftState *m_poSoftState{nullptr};
};
    
}
