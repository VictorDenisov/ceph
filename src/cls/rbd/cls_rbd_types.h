// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_RBD_TYPES_H
#define CEPH_CLS_RBD_TYPES_H

#include <boost/variant.hpp>
#include "include/int_types.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/stringify.h"
#include "include/utime.h"
#include <iosfwd>
#include <string>

#define RBD_GROUP_REF "rbd_group_ref"
#define RBD_GROUP_STATE "rbd_group_state"

namespace ceph { class Formatter; }

namespace cls {
namespace rbd {

static const uint32_t MAX_OBJECT_MAP_OBJECT_COUNT = 256000000;
static const string RBD_GROUP_IMAGE_KEY_PREFIX = "image_";

enum MirrorMode {
  MIRROR_MODE_DISABLED = 0,
  MIRROR_MODE_IMAGE    = 1,
  MIRROR_MODE_POOL     = 2
};

enum GroupImageLinkState {
  GROUP_IMAGE_LINK_STATE_ATTACHED,
  GROUP_IMAGE_LINK_STATE_INCOMPLETE
};

inline void encode(const GroupImageLinkState &state, bufferlist& bl,
		   uint64_t features=0)
{
  ::encode(static_cast<uint8_t>(state), bl);
}

inline void decode(GroupImageLinkState &state, bufferlist::iterator& it)
{
  uint8_t int_state;
  ::decode(int_state, it);
  state = static_cast<GroupImageLinkState>(int_state);
}

enum GroupState {
  GROUP_STATE_NORMAL,
  GROUP_STATE_CAPTURING_LOCK,
  GROUP_STATE_SNAPSHOTTING,
  GROUP_STATE_MAKING_INDIVIDUAL_SNAPS,
  GROUP_STATE_MAKING_PENDING_INDIVIDUAL_SNAP,
  GROUP_STATE_COMMITTING,
  GROUP_STATE_RELEASING_LOCK,
};

inline void encode(const GroupState &state, bufferlist& bl,
		   uint64_t features=0)
{
  ::encode(static_cast<uint8_t>(state), bl);
}

inline void decode(GroupState &state, bufferlist::iterator& it)
{
  uint8_t int_state;
  ::decode(int_state, it);
  state = static_cast<GroupState>(int_state);
}

struct MirrorPeer {
  MirrorPeer() {
  }
  MirrorPeer(const std::string &uuid, const std::string &cluster_name,
             const std::string &client_name, int64_t pool_id)
    : uuid(uuid), cluster_name(cluster_name), client_name(client_name),
      pool_id(pool_id) {
  }

  std::string uuid;
  std::string cluster_name;
  std::string client_name;
  int64_t pool_id = -1;

  inline bool is_valid() const {
    return (!uuid.empty() && !cluster_name.empty() && !client_name.empty());
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<MirrorPeer*> &o);

  bool operator==(const MirrorPeer &rhs) const;
};

std::ostream& operator<<(std::ostream& os, const MirrorMode& mirror_mode);
std::ostream& operator<<(std::ostream& os, const MirrorPeer& peer);

WRITE_CLASS_ENCODER(MirrorPeer);

enum MirrorImageState {
  MIRROR_IMAGE_STATE_DISABLING = 0,
  MIRROR_IMAGE_STATE_ENABLED   = 1
};

struct MirrorImage {
  MirrorImage() {}
  MirrorImage(const std::string &global_image_id, MirrorImageState state)
    : global_image_id(global_image_id), state(state) {}

  std::string global_image_id;
  MirrorImageState state = MIRROR_IMAGE_STATE_DISABLING;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<MirrorImage*> &o);

  bool operator==(const MirrorImage &rhs) const;
  bool operator<(const MirrorImage &rhs) const;
};

std::ostream& operator<<(std::ostream& os, const MirrorImageState& mirror_state);
std::ostream& operator<<(std::ostream& os, const MirrorImage& mirror_image);

WRITE_CLASS_ENCODER(MirrorImage);

enum MirrorImageStatusState {
  MIRROR_IMAGE_STATUS_STATE_UNKNOWN         = 0,
  MIRROR_IMAGE_STATUS_STATE_ERROR           = 1,
  MIRROR_IMAGE_STATUS_STATE_SYNCING         = 2,
  MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY = 3,
  MIRROR_IMAGE_STATUS_STATE_REPLAYING       = 4,
  MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY = 5,
  MIRROR_IMAGE_STATUS_STATE_STOPPED         = 6,
};

inline void encode(const MirrorImageStatusState &state, bufferlist& bl,
		   uint64_t features=0)
{
  ::encode(static_cast<uint8_t>(state), bl);
}

inline void decode(MirrorImageStatusState &state, bufferlist::iterator& it)
{
  uint8_t int_state;
  ::decode(int_state, it);
  state = static_cast<MirrorImageStatusState>(int_state);
}

struct MirrorImageStatus {
  MirrorImageStatus() {}
  MirrorImageStatus(MirrorImageStatusState state,
		    const std::string &description = "")
    : state(state), description(description) {}

  MirrorImageStatusState state = MIRROR_IMAGE_STATUS_STATE_UNKNOWN;
  std::string description;
  utime_t last_update;
  bool up = false;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void dump(Formatter *f) const;

  std::string state_to_string() const;

  static void generate_test_instances(std::list<MirrorImageStatus*> &o);

  bool operator==(const MirrorImageStatus &rhs) const;
};

std::ostream& operator<<(std::ostream& os, const MirrorImageStatus& status);
std::ostream& operator<<(std::ostream& os, const MirrorImageStatusState& state);

WRITE_CLASS_ENCODER(MirrorImageStatus);

struct GroupImageSpec {
  GroupImageSpec() {}

  GroupImageSpec(const std::string &image_id, int64_t pool_id)
    : image_id(image_id), pool_id(pool_id) {}

  static int from_key(const std::string &image_key, GroupImageSpec *spec);

  std::string image_id;
  int64_t pool_id = -1;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void dump(Formatter *f) const;

  std::string image_key();

};

WRITE_CLASS_ENCODER(GroupImageSpec);

struct GroupImageStatus {
  GroupImageStatus() {}
  GroupImageStatus(const std::string &image_id,
		   int64_t pool_id,
		   GroupImageLinkState state)
    : spec(image_id, pool_id), state(state) {}

  GroupImageStatus(GroupImageSpec spec,
		   GroupImageLinkState state)
    : spec(spec), state(state) {}

  GroupImageSpec spec;
  GroupImageLinkState state = GROUP_IMAGE_LINK_STATE_INCOMPLETE;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void dump(Formatter *f) const;

  std::string state_to_string() const;
};

WRITE_CLASS_ENCODER(GroupImageStatus);

struct GroupSpec {
  GroupSpec() {}
  GroupSpec(const std::string &group_id, int64_t pool_id)
    : group_id(group_id), pool_id(pool_id) {}

  std::string group_id;
  int64_t pool_id = -1;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void dump(Formatter *f) const;
  bool is_valid() const;
};

WRITE_CLASS_ENCODER(GroupSpec);

struct ImageSnapshotRef {
  int64_t pool;
  string image_id;
  snapid_t snap_id;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
};

WRITE_CLASS_ENCODER(ImageSnapshotRef);

struct GroupSnapshot {
  snapid_t id;
  string name;
  vector<ImageSnapshotRef> snaps;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
};

WRITE_CLASS_ENCODER(GroupSnapshot);

struct PendingImageSnapshot {
  int64_t pool;
  string image_id;
  string snap_name;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
};

WRITE_CLASS_ENCODER(PendingImageSnapshot);

enum SnapshotNamespaceType {
  SNAPSHOT_NAMESPACE_TYPE_USER = 0,
  SNAPSHOT_NAMESPACE_TYPE_GROUP = 1
};

struct UserSnapshotNamespace {
  static const uint32_t SNAPSHOT_NAMESPACE_TYPE = SNAPSHOT_NAMESPACE_TYPE_USER;

  UserSnapshotNamespace() {}

  void encode(bufferlist& bl) const {}
  void decode(__u8 version, bufferlist::iterator& it) {}
};

struct GroupSnapshotNamespace {
  static const uint32_t SNAPSHOT_NAMESPACE_TYPE = SNAPSHOT_NAMESPACE_TYPE_GROUP;

  GroupSnapshotNamespace() {}

  GroupSnapshotNamespace(int64_t _group_pool,
			 const string &_group_id,
			 const string &_snapshot_id) :group_pool(_group_pool),
						      group_id(_group_id),
						      snapshot_id(_snapshot_id) {}

  int64_t group_pool;
  string group_id;
  string snapshot_id;

  void encode(bufferlist& bl) const {
    ::encode(group_pool, bl);
    ::encode(group_id, bl);
    ::encode(snapshot_id, bl);
  }

  void decode(__u8 version, bufferlist::iterator& it) {
    ::decode(group_pool, it);
    ::decode(group_id, it);
    ::decode(snapshot_id, it);
  }
};

struct UnknownSnapshotNamespace {
  static const uint32_t SNAPSHOT_NAMESPACE_TYPE = static_cast<uint32_t>(-1);

  UnknownSnapshotNamespace() {}

  void encode(bufferlist& bl) const {}
  void decode(__u8 version, bufferlist::iterator& it) {}
};

typedef boost::variant<UserSnapshotNamespace, GroupSnapshotNamespace, UnknownSnapshotNamespace> SnapshotNamespace;

class EncodeSnapshotTypeVisitor : public boost::static_visitor<void> {
public:
  explicit EncodeSnapshotTypeVisitor(bufferlist &bl) : m_bl(bl) {
  }

  template <typename T>
  inline void operator()(const T& t) const {
    ::encode(static_cast<uint32_t>(T::SNAPSHOT_NAMESPACE_TYPE), m_bl);
    t.encode(m_bl);
  }

private:
  bufferlist &m_bl;
};

class DecodeSnapshotTypeVisitor : public boost::static_visitor<void> {
public:
  DecodeSnapshotTypeVisitor(__u8 version, bufferlist::iterator &iter)
    : m_version(version), m_iter(iter) {
  }

  template <typename T>
  inline void operator()(T& t) const {
    t.decode(m_version, m_iter);
  }
private:
  __u8 m_version;
  bufferlist::iterator &m_iter;
};


} // namespace rbd
} // namespace cls

using cls::rbd::encode;
using cls::rbd::decode;

#endif // CEPH_CLS_RBD_TYPES_H
