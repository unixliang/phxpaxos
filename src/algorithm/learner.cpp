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

#include "learner.h"
#include "acceptor.h"
#include "crc32.h"
#include "cp_mgr.h"
#include "sm_base.h"
#include "group.h"

namespace phxpaxos
{

LearnerState :: LearnerState(const Config * poConfig, const LogStorage * poLogStorage)
    : m_oPaxosLog(poLogStorage)
{
    m_poConfig = (Config *)poConfig;

    Init();
}

LearnerState :: ~LearnerState()
{
}

void LearnerState :: Init()
{
}

bool LearnerState :: GetPendingCommit(uint64_t & llInstanceID, std::string & sValue)
{
    if (m_vecLearnStateList.empty() || m_vecLearnStateList.begin()->first != m_llLastCommitInstanceID + 1) {
        return false;
    }

    llInstanceID = m_vecLearnStateList.begin()->first;

    auto &&oLearnState = m_vecLearnStateList.begin()->second;

    // check checksum
    if (oLearnState.iLastChecksum && oLearnState.iLastChecksum != m_iLastChecksum) {
        PLGErr("checksum fail, InstanceID %lu my last checksum %u other last checksum %u", 
               llInstanceID, m_iLastChecksum, oLearnState.iLastChecksum);
        //BP->GetInstanceBP()->ChecksumLogicFail(); // TODO
        assert(oLearnState.iLastChecksum == m_iLastChecksum);
    }
    
    sValue = oLearnState.sValue;

    return true;
}

bool LearnerState :: FinishCommit(const uint64_t llCommitInstanceID, FinishCommitCallbackFunc fFinishCommitCallbackFunc)
{
    if (NoCheckpoint != m_llLastCommitInstanceID && llCommitInstanceID <= m_llLastCommitInstanceID) return true;

    while (!m_vecLearnStateList.empty())
    {
        auto llInstanceID = m_vecLearnStateList.begin()->first;
        auto &&oLearnState = m_vecLearnStateList.begin()->second;

        if (llInstanceID > llCommitInstanceID) break;
        if (llInstanceID != m_llLastCommitInstanceID + 1) return false;


        // broadcast msg to follower
        if (fFinishCommitCallbackFunc)
        {
            fFinishCommitCallbackFunc(llInstanceID, oLearnState, m_iLastChecksum);
        }

        // wrtie m_iLastChecksum into AcceptorStat, For possible follow-up SendLearnValue.
        {
            AcceptorStateData oState;
            oState.set_instanceid(llInstanceID);
            oState.set_acceptedvalue(oLearnState.sValue);
            oState.set_promiseid(oLearnState.oBallot.m_llProposalID);
            oState.set_promisenodeid(oLearnState.oBallot.m_llNodeID);
            oState.set_acceptedid(oLearnState.oBallot.m_llProposalID);
            oState.set_acceptednodeid(oLearnState.oBallot.m_llNodeID);
            oState.set_checksum(m_iLastChecksum);

            WriteOptions oWriteOptions;
            oWriteOptions.bSync = false;

            int ret = m_oPaxosLog.WriteState(oWriteOptions, m_poConfig->GetMyGroupIdx(), llInstanceID, oState);
            if (ret != 0)
            {
                PLGErr("LogStorage.WriteLog fail, InstanceID %lu ValueLen %zu ret %d",
                       llInstanceID, oLearnState.sValue.size(), ret);
                return false;
            }
        }

        // prepare for next round.
        m_iLastChecksum = crc32(m_iLastChecksum, (const uint8_t *)oLearnState.sValue.data(), oLearnState.sValue.size(), CRC32SKIP);
        ++m_llLastCommitInstanceID;
        m_vecLearnStateList.erase(m_vecLearnStateList.begin());
    }

    return true;
}


void LearnerState :: LearnValueWithoutWrite(const uint64_t llInstanceID, const BallotNumber & oLearnedBallot,
                                            const std::string & sValue, uint32_t iLastChecksum)
{
    auto &oLearnStat = m_vecLearnStateList[llInstanceID];
    oLearnStat.oBallot = oLearnedBallot;
    oLearnStat.sValue = sValue;
    oLearnStat.iLastChecksum = iLastChecksum;
}

int LearnerState :: LearnValue(const uint64_t llInstanceID, const BallotNumber & oLearnedBallot, 
                               const std::string & sValue, uint32_t iLastChecksum)
{
    AcceptorStateData oState;
    oState.set_instanceid(llInstanceID);
    oState.set_acceptedvalue(sValue);
    oState.set_promiseid(oLearnedBallot.m_llProposalID);
    oState.set_promisenodeid(oLearnedBallot.m_llNodeID);
    oState.set_acceptedid(oLearnedBallot.m_llProposalID);
    oState.set_acceptednodeid(oLearnedBallot.m_llNodeID);
    oState.set_checksum(iLastChecksum);

    WriteOptions oWriteOptions;
    oWriteOptions.bSync = false;

    int ret = m_oPaxosLog.WriteState(oWriteOptions, m_poConfig->GetMyGroupIdx(), llInstanceID, oState);
    if (ret != 0)
    {
        PLGErr("LogStorage.WriteLog fail, InstanceID %lu ValueLen %zu ret %d",
                llInstanceID, sValue.size(), ret);
        return ret;
    }

    LearnValueWithoutWrite(llInstanceID, oLearnedBallot, sValue, iLastChecksum);

    PLGDebug("OK, InstanceID %lu ValueLen %zu",
            llInstanceID, sValue.size());

    return 0;
}

uint64_t LearnerState :: GetLastCommitInstanceID()
{
    return m_llLastCommitInstanceID;
}



//////////////////////////////////////////////////////////////////////////////

Learner :: Learner(
        const Config * poConfig, 
        const MsgTransport * poMsgTransport,
        Group * poGroup,
        const LogStorage * poLogStorage,
        const IOLoop * poIOLoop,
        const CheckpointMgr * poCheckpointMgr,
        const SMFac * poSMFac)
    : Base(poConfig, poMsgTransport, poGroup), m_oLearnerState(poConfig, poLogStorage),
    m_poGroup(poGroup),
    m_oPaxosLog(poLogStorage), m_oLearnerSender((Config *)poConfig, this, &m_oPaxosLog),
    m_oCheckpointReceiver((Config *)poConfig, (LogStorage *)poLogStorage)
{
    SetInstanceID(m_oLearnerState.GetLastCommitInstanceID() + 1);

    m_oLearnerState.Init();

    m_iAskforlearn_noopTimerID = 0;
    m_poIOLoop = (IOLoop *)poIOLoop;

    m_poCheckpointMgr = (CheckpointMgr *)poCheckpointMgr;
    m_poSMFac = (SMFac *)poSMFac;
    m_poCheckpointSender = nullptr;

    m_llHighestSeenInstanceID = 0;
    m_iHighestSeenInstanceID_FromNodeID = nullnode;

    m_bIsIMLearning = false;

    m_llLastAckInstanceID = 0;
}

Learner :: ~Learner()
{
    delete m_poCheckpointSender;
}

void Learner :: StartLearnerSender()
{
    m_oLearnerSender.start();
}


////////////////////////////////////////////////////////////////

void Learner :: Stop()
{
    m_oLearnerSender.Stop();
    if (m_poCheckpointSender != nullptr)
    {
        m_poCheckpointSender->Stop();
    }
}

////////////////////////////////////////////////////////////////

const bool Learner :: IsIMLatest()
{
    return (GetInstanceID() + m_poConfig->GetWindowSize()) >= m_llHighestSeenInstanceID;
}

const uint64_t Learner :: GetSeenLatestInstanceID()
{
    return m_llHighestSeenInstanceID;
}

void Learner :: SetSeenInstanceID(const uint64_t llInstanceID, const nodeid_t llFromNodeID)
{
    if (llInstanceID > m_llHighestSeenInstanceID)
    {
        m_llHighestSeenInstanceID = llInstanceID;
        m_iHighestSeenInstanceID_FromNodeID = llFromNodeID;
    }
}

//////////////////////////////////////////////////////////////

void Learner :: Reset_AskforLearn_Noop(const int iTimeout)
{
    if (m_iAskforlearn_noopTimerID > 0)
    {
        m_poIOLoop->RemoveTimer(m_iAskforlearn_noopTimerID);
    }

    m_poIOLoop->AddTimer(iTimeout, [this](const uint32_t iTimerID)->void {
                                       // Timer_Learner_Askforlearn_noop
                                       AskforLearn_Noop();
                                   }, m_iAskforlearn_noopTimerID);
}

void Learner :: AskforLearn_Noop(const bool bIsStart)
{
    Reset_AskforLearn_Noop();

    m_bIsIMLearning = false;

    m_poCheckpointMgr->ExitCheckpointMode();

    AskforLearn();
    
    if (bIsStart)
    {
        AskforLearn();
    }
}

///////////////////////////////////////////////////////////////

void Learner :: AskforLearn()
{
    BP->GetLearnerBP()->AskforLearn();

    PLGHead("(unix) START. InstanceID %lu", GetInstanceID());

    PaxosMsg oPaxosMsg;

    oPaxosMsg.set_instanceid(GetInstanceID());
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_AskforLearn);

    if (m_poConfig->IsIMFollower())
    {
        //this is not proposal nodeid, just use this val to bring followto nodeid info.
        oPaxosMsg.set_proposalnodeid(m_poConfig->GetFollowToNodeID());
    }

    PLGHead("END InstanceID %lu MyNodeID %lu", oPaxosMsg.instanceid(), oPaxosMsg.nodeid());

    BroadcastMessage(oPaxosMsg, BroadcastMessage_Type_RunSelf_None, Message_SendType_TCP);
    BroadcastMessageToTempNode(oPaxosMsg, Message_SendType_UDP);
}

void Learner :: OnAskforLearn(const PaxosMsg & oPaxosMsg)
{
    BP->GetLearnerBP()->OnAskforLearn();
    
    PLGHead("START Msg.InstanceID %lu Now.InstanceID %lu Msg.from_nodeid %lu MinChosenInstanceID %lu", 
            oPaxosMsg.instanceid(), GetInstanceID(), oPaxosMsg.nodeid(),
            m_poCheckpointMgr->GetMinChosenInstanceID());
    
    SetSeenInstanceID(oPaxosMsg.instanceid(), oPaxosMsg.nodeid());

    if (oPaxosMsg.proposalnodeid() == m_poConfig->GetMyNodeID())
    {
        //Found a node follow me.
        PLImp("Found a node %lu follow me.", oPaxosMsg.nodeid());
        m_poConfig->AddFollowerNode(oPaxosMsg.nodeid());
    }
    
    if (oPaxosMsg.instanceid() >= GetInstanceID())
    //if (oPaxosMsg.instanceid() > GetInstanceID()) // (unix) from-node need to know whether the instance of oPaxosMsg.instanceid is chosen
    {
        return;
    }

    if (oPaxosMsg.instanceid() >= m_poCheckpointMgr->GetMinChosenInstanceID())
    {
        if (!m_oLearnerSender.Prepare(oPaxosMsg.instanceid(), oPaxosMsg.nodeid()))
        {
            BP->GetLearnerBP()->OnAskforLearnGetLockFail();

            PLGErr("LearnerSender working for others.");

            if (oPaxosMsg.instanceid() == (GetInstanceID() - 1))
            {
                PLGImp("InstanceID only difference one, just send this value to other.");
                //send one value
                AcceptorStateData oState;
                int ret = m_oPaxosLog.ReadState(m_poConfig->GetMyGroupIdx(), oPaxosMsg.instanceid(), oState);
                if (ret == 0)
                {
                    BallotNumber oBallot(oState.acceptedid(), oState.acceptednodeid());
                    SendLearnValue(oPaxosMsg.nodeid(), oPaxosMsg.instanceid(), oBallot, oState.acceptedvalue(), 0, false);
                }
            }
            
            return;
        }
    }
    
    SendNowInstanceID(oPaxosMsg.instanceid(), oPaxosMsg.nodeid());
}

void Learner :: SendNowInstanceID(const uint64_t llInstanceID, const nodeid_t iSendNodeID)
{
    BP->GetLearnerBP()->SendNowInstanceID();

    PaxosMsg oPaxosMsg;
    oPaxosMsg.set_instanceid(llInstanceID);
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_SendNowInstanceID);
    oPaxosMsg.set_nowinstanceid(GetInstanceID());
    oPaxosMsg.set_minchoseninstanceid(m_poCheckpointMgr->GetMinChosenInstanceID());

    if ((GetInstanceID() - llInstanceID) > 50)
    {
        //instanceid too close not need to send vsm/master checkpoint. 
        string sSystemVariablesCPBuffer;
        int ret = m_poConfig->GetSystemVSM()->GetCheckpointBuffer(sSystemVariablesCPBuffer);
        if (ret == 0)
        {
            oPaxosMsg.set_systemvariables(sSystemVariablesCPBuffer);
        }

        string sMasterVariablesCPBuffer;
        if (m_poConfig->GetMasterSM() != nullptr)
        {
            int ret = m_poConfig->GetMasterSM()->GetCheckpointBuffer(sMasterVariablesCPBuffer);
            if (ret == 0)
            {
                oPaxosMsg.set_mastervariables(sMasterVariablesCPBuffer);
            }
        }
    }

    SendMessage(iSendNodeID, oPaxosMsg);
}

void Learner :: OnSendNowInstanceID(const PaxosMsg & oPaxosMsg)
{
    BP->GetLearnerBP()->OnSendNowInstanceID();

    PLGHead("START Msg.InstanceID %lu Now.InstanceID %lu Msg.from_nodeid %lu Msg.MaxInstanceID %lu systemvariables_size %zu mastervariables_size %zu",
            oPaxosMsg.instanceid(), GetInstanceID(), oPaxosMsg.nodeid(), oPaxosMsg.nowinstanceid(), 
            oPaxosMsg.systemvariables().size(), oPaxosMsg.mastervariables().size());

    SetSeenInstanceID(oPaxosMsg.nowinstanceid(), oPaxosMsg.nodeid()); // TODO: use now instancdid or max chosen instance id?

    bool bSystemVariablesChange = false;
    int ret = m_poConfig->GetSystemVSM()->UpdateByCheckpoint(oPaxosMsg.systemvariables(), bSystemVariablesChange);
    if (ret == 0 && bSystemVariablesChange)
    {
        PLGHead("SystemVariables changed!, all thing need to reflesh, so skip this msg");
        return;
    }

    bool bMasterVariablesChange = false;
    if (m_poConfig->GetMasterSM() != nullptr)
    {
        ret = m_poConfig->GetMasterSM()->UpdateByCheckpoint(oPaxosMsg.mastervariables(), bMasterVariablesChange);
        if (ret == 0 && bMasterVariablesChange)
        {
            PLGHead("MasterVariables changed!");
        }
    }

    if (oPaxosMsg.instanceid() != GetInstanceID())
    {
        PLGErr("Lag msg, skip");
        return;
    }

    if (oPaxosMsg.nowinstanceid() <= GetInstanceID())
    {
        PLGErr("Lag msg, skip");
        return;
    }

    if (oPaxosMsg.minchoseninstanceid() > GetInstanceID())
    {
        BP->GetCheckpointBP()->NeedAskforCheckpoint();

        PLGHead("my instanceid %lu small than other's minchoseninstanceid %lu, other nodeid %lu",
                GetInstanceID(), oPaxosMsg.minchoseninstanceid(), oPaxosMsg.nodeid());

        AskforCheckpoint(oPaxosMsg.nodeid());
    }
    else if (!m_bIsIMLearning)
    {
        ComfirmAskForLearn(oPaxosMsg.nodeid());
    }
}

////////////////////////////////////////////

void Learner :: ComfirmAskForLearn(const nodeid_t iSendNodeID)
{
    BP->GetLearnerBP()->ComfirmAskForLearn();

    PLGHead("START");

    PaxosMsg oPaxosMsg;

    oPaxosMsg.set_instanceid(GetInstanceID());
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_ComfirmAskforLearn);

    PLGHead("END InstanceID %lu MyNodeID %lu", GetInstanceID(), oPaxosMsg.nodeid());

    SendMessage(iSendNodeID, oPaxosMsg);

    m_bIsIMLearning = true;
}

void Learner :: OnComfirmAskForLearn(const PaxosMsg & oPaxosMsg)
{
    BP->GetLearnerBP()->OnComfirmAskForLearn();

    PLGHead("START Msg.InstanceID %lu Msg.from_nodeid %lu", oPaxosMsg.instanceid(), oPaxosMsg.nodeid());

    if (!m_oLearnerSender.Comfirm(oPaxosMsg.instanceid(), oPaxosMsg.nodeid()))
    {
        BP->GetLearnerBP()->OnComfirmAskForLearnGetLockFail();

        PLGErr("LearnerSender comfirm fail, maybe is lag msg");
        return;
    }

    PLGImp("OK, success comfirm");
}

int Learner :: SendLearnValue(
        const nodeid_t iSendNodeID,
        const uint64_t llLearnInstanceID,
        const BallotNumber & oLearnedBallot,
        const std::string & sLearnedValue,
        const uint32_t iChecksum,
        const bool bNeedAck)
{
    BP->GetLearnerBP()->SendLearnValue();

    PaxosMsg oPaxosMsg;
    
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_SendLearnValue);
    oPaxosMsg.set_instanceid(llLearnInstanceID);
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oPaxosMsg.set_proposalnodeid(oLearnedBallot.m_llNodeID);
    oPaxosMsg.set_proposalid(oLearnedBallot.m_llProposalID);
    oPaxosMsg.set_value(sLearnedValue);
    oPaxosMsg.set_lastchecksum(iChecksum);
    if (bNeedAck)
    {
        oPaxosMsg.set_flag(PaxosMsgFlagType_SendLearnValue_NeedAck);
    }

    return SendMessage(iSendNodeID, oPaxosMsg, Message_SendType_TCP);
}

void Learner :: OnSendLearnValue(const PaxosMsg & oPaxosMsg)
{
    BP->GetLearnerBP()->OnSendLearnValue();

    PLGHead("START Msg.InstanceID %lu Now.InstanceID %lu Msg.ballot_proposalid %lu Msg.ballot_nodeid %lu Msg.ValueSize %zu",
            oPaxosMsg.instanceid(), GetInstanceID(), oPaxosMsg.proposalid(), 
            oPaxosMsg.nodeid(), oPaxosMsg.value().size());

    if (oPaxosMsg.instanceid() > GetInstanceID())
    {
        PLGDebug("[Latest Msg] i can't learn");
        return;
    }

    if (oPaxosMsg.instanceid() < GetInstanceID())
    {
        PLGDebug("[Lag Msg] no need to learn");
    }
    else
    {
        //learn value
        BallotNumber oBallot(oPaxosMsg.proposalid(), oPaxosMsg.proposalnodeid());
        int ret = m_oLearnerState.LearnValue(oPaxosMsg.instanceid(), oBallot, oPaxosMsg.value(), oPaxosMsg.lastchecksum());
        if (ret != 0)
        {
            PLGErr("LearnState.LearnValue fail, ret %d", ret);
            return;
        }
        
        PLGHead("END LearnValue OK, proposalid %lu proposalid_nodeid %lu valueLen %zu", 
                oPaxosMsg.proposalid(), oPaxosMsg.nodeid(), oPaxosMsg.value().size());
    }

    if (oPaxosMsg.flag() == PaxosMsgFlagType_SendLearnValue_NeedAck)
    {
        //every time' when receive valid need ack learn value, reset noop timeout.
        Reset_AskforLearn_Noop();

        SendLearnValue_Ack(oPaxosMsg.nodeid());
    }
}

void Learner :: SendLearnValue_Ack(const nodeid_t iSendNodeID)
{
    PLGHead("START LastAck.Instanceid %lu Now.Instanceid %lu", m_llLastAckInstanceID, GetInstanceID());

    if (GetInstanceID() < m_llLastAckInstanceID + LearnerReceiver_ACK_LEAD)
    {
        PLGImp("No need to ack");
        return;
    }
    
    BP->GetLearnerBP()->SendLearnValue_Ack();

    m_llLastAckInstanceID = GetInstanceID();

    PaxosMsg oPaxosMsg;
    oPaxosMsg.set_instanceid(GetInstanceID());
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_SendLearnValue_Ack);
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());

    SendMessage(iSendNodeID, oPaxosMsg);

    PLGHead("End. ok");
}

void Learner :: OnSendLearnValue_Ack(const PaxosMsg & oPaxosMsg)
{
    BP->GetLearnerBP()->OnSendLearnValue_Ack();

    PLGHead("Msg.Ack.Instanceid %lu Msg.from_nodeid %lu", oPaxosMsg.instanceid(), oPaxosMsg.nodeid());

    m_oLearnerSender.Ack(oPaxosMsg.instanceid(), oPaxosMsg.nodeid());
}

//////////////////////////////////////////////////////////////

void Learner :: TransmitToFollower(uint64_t llInstanceID, const LearnerState::LearnState & oLearnState, uint32_t iLastChecksum)
{
    if (m_poConfig->GetMyFollowerCount() == 0)
    {
        return;
    }

    PaxosMsg oPaxosMsg;
    
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_SendLearnValue);
    oPaxosMsg.set_instanceid(llInstanceID);
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oPaxosMsg.set_proposalnodeid(oLearnState.oBallot.m_llNodeID);
    oPaxosMsg.set_proposalid(oLearnState.oBallot.m_llProposalID);
    oPaxosMsg.set_value(oLearnState.sValue);
    oPaxosMsg.set_lastchecksum(iLastChecksum);

    BroadcastMessageToFollower(oPaxosMsg, Message_SendType_TCP);

    PLGHead("ok");
}

void Learner :: ProposerSendSuccess(
        const uint64_t llLearnInstanceID,
        const uint64_t llProposalID,
        const uint32_t iLastChecksum,
        BroadcastMessage_Type iRunType)
{
    BP->GetLearnerBP()->ProposerSendSuccess();

    PaxosMsg oPaxosMsg;
    
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_ProposerSendSuccess);
    oPaxosMsg.set_instanceid(llLearnInstanceID);
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oPaxosMsg.set_proposalid(llProposalID);
    oPaxosMsg.set_lastchecksum(iLastChecksum);

    //run self first
    //BroadcastMessage(oPaxosMsg, BroadcastMessage_Type_RunSelf_First);
    BroadcastMessage(oPaxosMsg, iRunType);
}

void Learner :: OnProposerSendSuccess(const PaxosMsg & oPaxosMsg)
{
    BP->GetLearnerBP()->OnProposerSendSuccess();

    auto llInstanceID = oPaxosMsg.instanceid();
    auto poInstance = m_poGroup->GetInstance(llInstanceID);
    if (!poInstance) {
        PLGErr("Instance not found, InstanceID %lu",
               llInstanceID);
        return;
    }
    auto poAcceptor = poInstance->GetAcceptor();
    auto poAcceptorState = poAcceptor->GetAcceptorState();

    PLGHead("START Msg.InstanceID %lu Msg.ProposalID %lu State.AcceptedID %lu "
            "State.AcceptedNodeID %lu, Msg.from_nodeid %lu",
            llInstanceID, oPaxosMsg.proposalid(), 
            poAcceptorState->GetAcceptedBallot().m_llProposalID,
            poAcceptorState->GetAcceptedBallot().m_llNodeID, 
            oPaxosMsg.nodeid());

    if (poAcceptorState->GetAcceptedBallot().isnull())
    {
        //Not accept any yet.
        BP->GetLearnerBP()->OnProposerSendSuccessNotAcceptYet();
        PLGDebug("I haven't accpeted any proposal");
        return;
    }

    BallotNumber oBallot(oPaxosMsg.proposalid(), oPaxosMsg.nodeid());

    if (poAcceptorState->GetAcceptedBallot()
            != oBallot)
    {
        //Proposalid not same, this accept value maybe not chosen value.
        PLGDebug("ProposalBallot not same to AcceptedBallot");
        BP->GetLearnerBP()->OnProposerSendSuccessBallotNotSame();
        return;
    }

    //learn value.
    m_oLearnerState.LearnValueWithoutWrite(
            llInstanceID,
            oBallot,
            poAcceptorState->GetAcceptedValue(),
            oPaxosMsg.lastchecksum());
    
    BP->GetLearnerBP()->OnProposerSendSuccessSuccessLearn();

    PLGHead("END Learn value OK, value %zu", poAcceptorState->GetAcceptedValue().size());

    //TransmitToFollower();
}


bool Learner :: GetPendingCommit(uint64_t & llInstanceID, std::string & sValue)
{
    return m_oLearnerState.GetPendingCommit(llInstanceID, sValue);
}

bool Learner :: FinishCommit(uint64_t & llCommitInstanceID)
{
    bool ok = m_oLearnerState.FinishCommit(llCommitInstanceID, [this](uint64_t llInstanceID, const LearnerState::LearnState & oLearnState, uint32_t iLastChecksum)->void {
                                                                   // broadcast to node
                                                                   ProposerSendSuccess(llInstanceID, oLearnState.oBallot.m_llProposalID, iLastChecksum, BroadcastMessage_Type_RunSelf_None);
                                                                   // broadcast to follower
                                                                   TransmitToFollower(llInstanceID, oLearnState, iLastChecksum);
                                                               });

    SetInstanceID(m_oLearnerState.GetLastCommitInstanceID() + 1);

    PLGDebug("(unix) CommitInstanceID %lu LastCommitInstanceID %lu Learner.InstanceID %lu",
             llCommitInstanceID, m_oLearnerState.GetLastCommitInstanceID(), GetInstanceID());

    return ok;

}

////////////////////////////////////////////////////////////////////////

void Learner :: AskforCheckpoint(const nodeid_t iSendNodeID)
{
    PLGHead("START");

    int ret = m_poCheckpointMgr->PrepareForAskforCheckpoint(iSendNodeID);
    if (ret != 0)
    {
        return;
    }

    PaxosMsg oPaxosMsg;

    oPaxosMsg.set_instanceid(GetInstanceID());
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_AskforCheckpoint);

    PLGHead("END InstanceID %lu MyNodeID %lu", GetInstanceID(), oPaxosMsg.nodeid());
    
    SendMessage(iSendNodeID, oPaxosMsg);
}

void Learner :: OnAskforCheckpoint(const PaxosMsg & oPaxosMsg)
{
    CheckpointSender * poCheckpointSender = GetNewCheckpointSender(oPaxosMsg.nodeid());
    if (poCheckpointSender != nullptr)
    {
        poCheckpointSender->start();
        PLGHead("new checkpoint sender started, send to nodeid %lu", oPaxosMsg.nodeid());
    }
    else
    {
        PLGErr("Checkpoint Sender is running");
    }
}

int Learner :: SendCheckpointBegin(
        const nodeid_t iSendNodeID,
        const uint64_t llUUID,
        const uint64_t llSequence,
        const uint64_t llCheckpointInstanceID)
{
    CheckpointMsg oCheckpointMsg;

    oCheckpointMsg.set_msgtype(CheckpointMsgType_SendFile);
    oCheckpointMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oCheckpointMsg.set_flag(CheckpointSendFileFlag_BEGIN);
    oCheckpointMsg.set_uuid(llUUID);
    oCheckpointMsg.set_sequence(llSequence);
    oCheckpointMsg.set_checkpointinstanceid(llCheckpointInstanceID);

    PLGImp("END, SendNodeID %lu uuid %lu sequence %lu cpi %lu",
            iSendNodeID, llUUID, llSequence, llCheckpointInstanceID);

    return SendMessage(iSendNodeID, oCheckpointMsg, Message_SendType_TCP);
}

int Learner :: SendCheckpointEnd(
        const nodeid_t iSendNodeID,
        const uint64_t llUUID,
        const uint64_t llSequence,
        const uint64_t llCheckpointInstanceID)
{
    CheckpointMsg oCheckpointMsg;

    oCheckpointMsg.set_msgtype(CheckpointMsgType_SendFile);
    oCheckpointMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oCheckpointMsg.set_flag(CheckpointSendFileFlag_END);
    oCheckpointMsg.set_uuid(llUUID);
    oCheckpointMsg.set_sequence(llSequence);
    oCheckpointMsg.set_checkpointinstanceid(llCheckpointInstanceID);

    PLGImp("END, SendNodeID %lu uuid %lu sequence %lu cpi %lu",
            iSendNodeID, llUUID, llSequence, llCheckpointInstanceID);

    return SendMessage(iSendNodeID, oCheckpointMsg, Message_SendType_TCP);
}

int Learner :: SendCheckpoint(
        const nodeid_t iSendNodeID,
        const uint64_t llUUID,
        const uint64_t llSequence,
        const uint64_t llCheckpointInstanceID,
        const uint32_t iChecksum,
        const std::string & sFilePath,
        const int iSMID,
        const uint64_t llOffset,
        const std::string & sBuffer)
{
    CheckpointMsg oCheckpointMsg;

    oCheckpointMsg.set_msgtype(CheckpointMsgType_SendFile);
    oCheckpointMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oCheckpointMsg.set_flag(CheckpointSendFileFlag_ING);
    oCheckpointMsg.set_uuid(llUUID);
    oCheckpointMsg.set_sequence(llSequence);
    oCheckpointMsg.set_checkpointinstanceid(llCheckpointInstanceID);
    oCheckpointMsg.set_checksum(iChecksum);
    oCheckpointMsg.set_filepath(sFilePath);
    oCheckpointMsg.set_smid(iSMID);
    oCheckpointMsg.set_offset(llOffset);
    oCheckpointMsg.set_buffer(sBuffer);

    PLGImp("END, SendNodeID %lu uuid %lu sequence %lu cpi %lu checksum %u smid %d offset %lu buffsize %zu filepath %s",
            iSendNodeID, llUUID, llSequence, llCheckpointInstanceID, 
            iChecksum, iSMID, llOffset, sBuffer.size(), sFilePath.c_str());

    return SendMessage(iSendNodeID, oCheckpointMsg, Message_SendType_TCP);
}

int Learner :: OnSendCheckpoint_Begin(const CheckpointMsg & oCheckpointMsg)
{
    int ret = m_oCheckpointReceiver.NewReceiver(oCheckpointMsg.nodeid(), oCheckpointMsg.uuid());
    if (ret == 0)
    {
        PLGImp("NewReceiver ok");

        ret = m_poCheckpointMgr->SetMinChosenInstanceID(oCheckpointMsg.checkpointinstanceid());
        if (ret != 0)
        {
            PLGErr("SetMinChosenInstanceID fail, ret %d CheckpointInstanceID %lu",
                    ret, oCheckpointMsg.checkpointinstanceid());

            return ret;
        }
    }

    return ret;
}

int Learner :: OnSendCheckpoint_Ing(const CheckpointMsg & oCheckpointMsg)
{
    BP->GetCheckpointBP()->OnSendCheckpointOneBlock();
    return m_oCheckpointReceiver.ReceiveCheckpoint(oCheckpointMsg);
}

int Learner :: OnSendCheckpoint_End(const CheckpointMsg & oCheckpointMsg)
{
    if (!m_oCheckpointReceiver.IsReceiverFinish(oCheckpointMsg.nodeid(), 
                oCheckpointMsg.uuid(), oCheckpointMsg.sequence()))
    {
        PLGErr("receive end msg but receiver not finish");
        return -1;
    }
    
    BP->GetCheckpointBP()->ReceiveCheckpointDone();

    std::vector<StateMachine *> vecSMList = m_poSMFac->GetSMList();
    for (auto & poSM : vecSMList)
    {
        if (poSM->SMID() == SYSTEM_V_SMID
                || poSM->SMID() == MASTER_V_SMID)
        {
            //system variables sm no checkpoint
            //master variables sm no checkpoint
            continue;
        }

        string sTmpDirPath = m_oCheckpointReceiver.GetTmpDirPath(poSM->SMID());
        std::vector<std::string> vecFilePathList;

        int ret = FileUtils :: IterDir(sTmpDirPath, vecFilePathList);
        if (ret != 0)
        {
            PLGErr("IterDir fail, dirpath %s", sTmpDirPath.c_str());
        }

        if (vecFilePathList.size() == 0)
        {
            PLGImp("this sm %d have no checkpoint", poSM->SMID());
            continue;
        }
        
        ret = poSM->LoadCheckpointState(
                m_poConfig->GetMyGroupIdx(),
                sTmpDirPath,
                vecFilePathList,
                oCheckpointMsg.checkpointinstanceid());
        if (ret != 0)
        {
            BP->GetCheckpointBP()->ReceiveCheckpointAndLoadFail();
            return ret;
        }

    }

    BP->GetCheckpointBP()->ReceiveCheckpointAndLoadSucc();
    PLGImp("All sm load state ok, start to exit process");
    exit(-1);

    return 0;
}

void Learner :: OnSendCheckpoint(const CheckpointMsg & oCheckpointMsg)
{
    PLGHead("START uuid %lu flag %d sequence %lu cpi %lu checksum %u smid %d offset %lu buffsize %zu filepath %s",
            oCheckpointMsg.uuid(), oCheckpointMsg.flag(), oCheckpointMsg.sequence(), 
            oCheckpointMsg.checkpointinstanceid(), oCheckpointMsg.checksum(), oCheckpointMsg.smid(), 
            oCheckpointMsg.offset(), oCheckpointMsg.buffer().size(), oCheckpointMsg.filepath().c_str());

    int ret = 0;
    
    if (oCheckpointMsg.flag() == CheckpointSendFileFlag_BEGIN)
    {
        ret = OnSendCheckpoint_Begin(oCheckpointMsg);
    }
    else if (oCheckpointMsg.flag() == CheckpointSendFileFlag_ING)
    {
        ret = OnSendCheckpoint_Ing(oCheckpointMsg);
    }
    else if (oCheckpointMsg.flag() == CheckpointSendFileFlag_END)
    {
        ret = OnSendCheckpoint_End(oCheckpointMsg);
    }

    if (ret != 0)
    {
        PLGErr("[FAIL] reset checkpoint receiver and reset askforlearn");

        m_oCheckpointReceiver.Reset();

        Reset_AskforLearn_Noop(5000);
        SendCheckpointAck(oCheckpointMsg.nodeid(), oCheckpointMsg.uuid(), oCheckpointMsg.sequence(), CheckpointSendFileAckFlag_Fail);
    }
    else
    {
        SendCheckpointAck(oCheckpointMsg.nodeid(), oCheckpointMsg.uuid(), oCheckpointMsg.sequence(), CheckpointSendFileAckFlag_OK);
        Reset_AskforLearn_Noop(120000);
    }
}

int Learner :: SendCheckpointAck(
        const nodeid_t iSendNodeID,
        const uint64_t llUUID,
        const uint64_t llSequence,
        const int iFlag)
{
    CheckpointMsg oCheckpointMsg;

    oCheckpointMsg.set_msgtype(CheckpointMsgType_SendFile_Ack);
    oCheckpointMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oCheckpointMsg.set_uuid(llUUID);
    oCheckpointMsg.set_sequence(llSequence);
    oCheckpointMsg.set_flag(iFlag);

    return SendMessage(iSendNodeID, oCheckpointMsg, Message_SendType_TCP);
}

void Learner :: OnSendCheckpointAck(const CheckpointMsg & oCheckpointMsg)
{
    PLGHead("START flag %d", oCheckpointMsg.flag());

    if (m_poCheckpointSender != nullptr && !m_poCheckpointSender->IsEnd())
    {
        if (oCheckpointMsg.flag() == CheckpointSendFileAckFlag_OK)
        {
            m_poCheckpointSender->Ack(oCheckpointMsg.nodeid(), oCheckpointMsg.uuid(), oCheckpointMsg.sequence());
        }
        else
        {
            m_poCheckpointSender->End();
        }
    }
}

CheckpointSender * Learner :: GetNewCheckpointSender(const nodeid_t iSendNodeID)
{
    if (m_poCheckpointSender != nullptr)
    {
        if (m_poCheckpointSender->IsEnd())
        {
            m_poCheckpointSender->join();
            delete m_poCheckpointSender;
            m_poCheckpointSender = nullptr;
        }
    }

    if (m_poCheckpointSender == nullptr)
    {
        m_poCheckpointSender = new CheckpointSender(iSendNodeID, m_poConfig, this, m_poSMFac, m_poCheckpointMgr);
        return m_poCheckpointSender;
    }

    return nullptr;
}


}



