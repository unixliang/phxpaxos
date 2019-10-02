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

#include "soft_state.h"

#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "crc32.h"

#include "db.h"
#include "paxos_msg.pb.h"

namespace phxpaxos
{

SoftState::SoftState(const Options & oOptions, const int iGroupIdx)
  : m_iMyGroupIdx(iGroupIdx), m_iMaxWindowSize(oOptions.iMaxWindowSize) {
}

void SoftState :: UpdateOnPersist(const uint64_t llInstanceID, const AcceptorStateData &oState) {
  // promise ballot
  BallotNumber oPromiseBallot(oState.promiseid(), oState.promisenodeid());
  SetPromiseBallot(llInstanceID, oPromiseBallot);

  PLG1Debug("(unix) InstanceID %lu LastChecksum %u", llInstanceID, oState.checksum());

  // md5
  if (oState.checksum()) {
    m_mapInstanceID2LastChecksum[llInstanceID] = oState.checksum();
  }

  // HighestOtherProposalID
  SetOtherProposalID(oState.promiseid());
}

void SoftState :: UpdateOnCommit(const uint64_t llInstanceID, const std::string &sValue) {
  // md5
  auto &&it = m_mapInstanceID2LastChecksum.find(llInstanceID);
  if (m_mapInstanceID2LastChecksum.end() != it) {
    PLG1Debug("(unix) InstanceID %lu LastChecksum %u update by peer", llInstanceID, m_iLastChecksum);
    m_iLastChecksum = it->second;
  } else {
    PLG1Debug("(unix) InstanceID %lu LastChecksum %u update by local", llInstanceID, m_iLastChecksum);
    m_mapInstanceID2LastChecksum[llInstanceID] = m_iLastChecksum;
  }

  {
    uint32_t iValueChecksum = crc32(0, (const uint8_t *)sValue.data(), sValue.size(), CRC32SKIP);
    PLG1Debug("(unix) InstanceID %lu iValueChecksum %u valuesize %u", llInstanceID, iValueChecksum, sValue.size());
  }

  uint32_t iChecksum{0};
  if (m_iLastChecksum || 0 == llInstanceID) {
    iChecksum = crc32(m_iLastChecksum, (const uint8_t *)sValue.data(), sValue.size(), CRC32SKIP);
  }
  m_iLastChecksum = iChecksum;
}

void SoftState::OnMinChosenInstanceIDUpdate(const uint64_t llMinChosenInstanceID) {
  while (m_mapInstanceID2LastChecksum.begin() != m_mapInstanceID2LastChecksum.end() && m_mapInstanceID2LastChecksum.begin()->first < llMinChosenInstanceID) {
    m_mapInstanceID2LastChecksum.erase(m_mapInstanceID2LastChecksum.begin());
  }
}

void SoftState :: SetPromiseBallot(const uint64_t llInstanceID, const BallotNumber &oBallotNumber)
{
  uint64_t llEndPromiseInstanceID{NoCheckpoint};
  BallotNumber oPromiseBallotNumber = GetPromiseBallot(llInstanceID, llEndPromiseInstanceID);

  if (!(oBallotNumber > oPromiseBallotNumber)) return;

  m_mapInstanceID2PromiseBallot[llInstanceID] = oBallotNumber;

  PLG1Debug("(unix) set new PromiseBallot(ProposalID: %lu, NodeID: %lu). InstanceID %lu", oBallotNumber.m_llProposalID, oBallotNumber.m_llNodeID, llInstanceID);

  while (m_mapInstanceID2PromiseBallot.size() > m_iMaxWindowSize)
  {
    m_mapInstanceID2PromiseBallot.erase(m_mapInstanceID2PromiseBallot.begin());
  }
}

BallotNumber SoftState :: GetPromiseBallot(const uint64_t llInstanceID, uint64_t & llEndPromiseInstanceID) const
{
  llEndPromiseInstanceID = NoCheckpoint;

  auto it = m_mapInstanceID2PromiseBallot.upper_bound(llInstanceID);
  if (m_mapInstanceID2PromiseBallot.end() != it) {
    llEndPromiseInstanceID = it->first;
  }
  if (m_mapInstanceID2PromiseBallot.begin() == it) {
    PLG1Debug("(unix) PromiseBallot empty. InstanceID %lu EndPromiseInstanceID %lu", llInstanceID, llEndPromiseInstanceID);
    return BallotNumber();
  }

  --it;

  PLG1Debug("(unix) PromiseBallot(ProposalID: %lu, NodeID: %lu). InstanceID %lu EndPromiseInstanceID %lu", it->second.m_llProposalID, it->second.m_llNodeID, llInstanceID, llEndPromiseInstanceID);

  return it->second;
}


uint32_t SoftState::GetLastChecksum(const uint64_t llInstanceID) {
  uint32_t iLastChecksum{0};
  auto &&it = m_mapInstanceID2LastChecksum.find(llInstanceID);
  if (m_mapInstanceID2LastChecksum.end() != it) {
    iLastChecksum = it->second;
  }
  PLG1Debug("(unix) InstanceID %lu LastChecksum %u", llInstanceID, iLastChecksum);
  return iLastChecksum;
}


void SoftState::SetOtherProposalID(const uint64_t llOtherProposalID) {
  if (llOtherProposalID > m_llHighestOtherProposalID)
    m_llHighestOtherProposalID = llOtherProposalID;
}

uint64_t SoftState::GenMyProposalID() {
  return m_llHighestOtherProposalID + 1;
}

void SoftState::SetEndPromiseInstanceID(const uint64_t llEndInstanceID) {
  m_setEndPromiseInstanceID.insert(llEndInstanceID);
  while (m_setEndPromiseInstanceID.size() > m_iMaxWindowSize)
  {
    m_setEndPromiseInstanceID.erase(m_setEndPromiseInstanceID.begin());
  }
}

bool SoftState::IsPromiseEnd(const uint64_t llInstanceID) {
  if (!m_bHasJudgePromiseEnd) {
    m_bHasJudgePromiseEnd = true;
    return true;
  }
  if (m_setEndPromiseInstanceID.empty()) { // not prepare yet
    return true;
  }
  return m_setEndPromiseInstanceID.end() != m_setEndPromiseInstanceID.find(llInstanceID);
}



//////////////////////////////////////////////////////


int MultiSoftState::Init(const Options & oOptions) {
  for (int iGroupIdx = 0; iGroupIdx < oOptions.iGroupCount; iGroupIdx++) {
    PLDebug("(unix) iGroupIdx %d", iGroupIdx);
    m_vecSoftStateList.push_back(SoftState(oOptions, iGroupIdx));
  }

  return 0;
}

SoftState *MultiSoftState::GetSoftState(const int iGroupIdx) {
  if (iGroupIdx >= m_vecSoftStateList.size()) {
    PLErr("iGroupIdx %d < size %u", iGroupIdx, m_vecSoftStateList.size());
    return nullptr;
  }
  return &m_vecSoftStateList[iGroupIdx];
}


}
