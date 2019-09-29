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
#include <string>
#include <map>
#include "comm_include.h"
#include "paxos_msg.pb.h"

namespace phxpaxos
{

class SoftState {
public:
  SoftState(const Options & oOptions, const int iGroupIdx);
  ~SoftState() = default;

public:
  void UpdateOnPersist(const uint64_t llInstanceID, const AcceptorStateData &oState);
  void UpdateOnCommit(const uint64_t llInstanceID, const std::string &sValue);

public:
  void OnMinChosenInstanceIDUpdate(const uint64_t llMinChosenInstanceID);

public:
  BallotNumber GetPromiseBallot(const uint64_t llInstanceID, uint64_t & llEndPromiseInstanceID) const;
  void SetPromiseBallot(const uint64_t llInstanceID, const BallotNumber &oBallotNumber);

public:
  uint32_t GetLastChecksum(const uint64_t llInstanceID);
  //void SetLastChecksum(const uint64_t llInstanceID, const uint32_t iLastChecksum);

public:
  void SetOtherProposalID(const uint64_t llOtherProposalID);
  uint64_t GetHighestOtherProposalID();

private:
  int m_iMyGroupIdx{-1};
  int m_iMaxWindowSize{0};

  std::map<uint64_t, BallotNumber> m_mapInstanceID2PromiseBallot; // for acceptor OnPrepare

  uint32_t m_iLastChecksum{0};
  std::map<uint64_t, uint32_t> m_mapInstanceID2LastChecksum;

  uint64_t m_llHighestOtherProposalID{0};
};

class MultiSoftState {
public:
  MultiSoftState() = default;
  ~MultiSoftState() = default;

  int Init(const Options & oOptions);
  SoftState *GetSoftState(const int iGroupIdx);

private:
  std::vector<SoftState> m_vecSoftStateList;

};


}
