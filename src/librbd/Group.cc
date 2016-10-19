// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "common/errno.h"

#include "librbd/AioCompletion.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/Group.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Group: "

using std::map;
using std::pair;
using std::set;
using std::string;
using std::vector;
// list binds to list() here, so std::list is explicitly used below

using ceph::bufferlist;
using librados::snap_t;
using librados::IoCtx;
using librados::Rados;

namespace librbd {

// Consistency groups functions

int group_create(librados::IoCtx& io_ctx, const char *group_name)
{
  CephContext *cct = (CephContext *)io_ctx.cct();

  Rados rados(io_ctx);
  uint64_t bid = rados.get_instance_id();

  uint32_t extra = rand() % 0xFFFFFFFF;
  ostringstream bid_ss;
  bid_ss << std::hex << bid << std::hex << extra;
  string id = bid_ss.str();

  ldout(cct, 2) << "adding consistency group to directory..." << dendl;

  int r = cls_client::group_dir_add(&io_ctx, RBD_GROUP_DIRECTORY, group_name, id);
  if (r < 0) {
    lderr(cct) << "error adding consistency group to directory: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }
  string header_oid = util::group_header_name(id);

  r = cls_client::group_create(&io_ctx, header_oid);
  if (r < 0) {
    lderr(cct) << "error writing header: " << cpp_strerror(r) << dendl;
    goto err_remove_from_dir;
  }

  return 0;

err_remove_from_dir:
  int remove_r = cls_client::group_dir_remove(&io_ctx, RBD_GROUP_DIRECTORY,
					      group_name, id);
  if (remove_r < 0) {
    lderr(cct) << "error cleaning up consistency group from rbd_directory "
	       << "object after creation failed: " << cpp_strerror(remove_r)
	       << dendl;
  }

  return r;
}

int group_remove(librados::IoCtx& io_ctx, const char *group_name)
{
  CephContext *cct((CephContext *)io_ctx.cct());
  ldout(cct, 20) << "group_remove " << &io_ctx << " " << group_name << dendl;

  std::vector<group_image_status_t> images;
  int r = group_image_list(io_ctx, group_name, images);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error listing group images" << dendl;
    return r;
  }

  for (auto i : images) {
    librados::Rados rados(io_ctx);
    IoCtx image_ioctx;
    rados.ioctx_create2(i.pool, image_ioctx);
    r = group_image_remove(io_ctx, group_name, image_ioctx, i.name.c_str());
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error removing image from a group" << dendl;
      return r;
    }
  }

  std::string group_id;
  r = cls_client::dir_get_id(&io_ctx, RBD_GROUP_DIRECTORY,
			     std::string(group_name), &group_id);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error getting id of group" << dendl;
    return r;
  }

  string header_oid = util::group_header_name(group_id);

  r = io_ctx.remove(header_oid);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error removing header: " << cpp_strerror(-r) << dendl;
    return r;
  }

  r = cls_client::group_dir_remove(&io_ctx, RBD_GROUP_DIRECTORY,
				       group_name, group_id);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error removing group from directory" << dendl;
    return r;
  }

  return 0;
}

int group_list(IoCtx& io_ctx, vector<string>& names)
{
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "group_list " << &io_ctx << dendl;

  int max_read = 1024;
  string last_read = "";
  int r;
  do {
    map<string, string> groups;
    r = cls_client::group_dir_list(&io_ctx, RBD_GROUP_DIRECTORY, last_read, max_read, &groups);
    if (r < 0) {
      lderr(cct) << "error listing group in directory: "
		 << cpp_strerror(r) << dendl;
      return r;
    }
    for (pair<string, string> group : groups) {
      names.push_back(group.first);
    }
    if (!groups.empty()) {
      last_read = groups.rbegin()->first;
    }
    r = groups.size();
  } while (r == max_read);

  return 0;
}

int group_image_add(librados::IoCtx& group_ioctx, const char *group_name,
		    librados::IoCtx& image_ioctx, const char *image_name)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  ldout(cct, 20) << "group_image_add " << &group_ioctx
		 << " group name " << group_name << " image "
		 << &image_ioctx << " name " << image_name << dendl;

  string group_id;

  int r = cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY, group_name, &group_id);
  if (r < 0) {
    lderr(cct) << "error reading consistency group id object: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }
  string group_header_oid = util::group_header_name(group_id);


  ldout(cct, 20) << "adding image to group name " << group_name
		 << " group id " << group_header_oid << dendl;

  string image_id;

  r = cls_client::dir_get_id(&image_ioctx, RBD_DIRECTORY, image_name, &image_id);
  if (r < 0) {
    lderr(cct) << "error reading image id object: "
	       << cpp_strerror(-r) << dendl;
    return r;
  }

  string image_header_oid = util::header_name(image_id);

  ldout(cct, 20) << "adding image " << image_name
		 << " image id " << image_header_oid << dendl;

  cls::rbd::GroupImageStatus incomplete_st(image_id, image_ioctx.get_id(),
				cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE);
  cls::rbd::GroupImageStatus attached_st(image_id, image_ioctx.get_id(),
				  cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED);

  r = cls_client::group_image_set(&group_ioctx, group_header_oid,
				  incomplete_st);

  cls::rbd::GroupSpec group_spec(group_id, group_ioctx.get_id());

  if (r < 0) {
    lderr(cct) << "error adding image reference to consistency group: "
	       << cpp_strerror(-r) << dendl;
    return r;
  }

  r = cls_client::image_add_group(&image_ioctx, image_header_oid,
					 group_spec);
  if (r < 0) {
    lderr(cct) << "error adding group reference to image: "
	       << cpp_strerror(-r) << dendl;
    cls::rbd::GroupImageSpec spec(image_id, image_ioctx.get_id());
    cls_client::group_image_remove(&group_ioctx, group_header_oid, spec);
    // Ignore errors in the clean up procedure.
    return r;
  }

  r = cls_client::group_image_set(&group_ioctx, group_header_oid,
				  attached_st);

  return r;
}

int group_image_remove(librados::IoCtx& group_ioctx, const char *group_name,
		       librados::IoCtx& image_ioctx, const char *image_name)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  ldout(cct, 20) << "group_remove_image " << &group_ioctx
		 << " group name " << group_name << " image "
		 << &image_ioctx << " name " << image_name << dendl;

  string group_id;

  int r = cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY, group_name, &group_id);
  if (r < 0) {
    lderr(cct) << "error reading consistency group id object: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }
  string group_header_oid = util::group_header_name(group_id);

  ldout(cct, 20) << "adding image to group name " << group_name
		 << " group id " << group_header_oid << dendl;

  string image_id;
  r = cls_client::dir_get_id(&image_ioctx, RBD_DIRECTORY, image_name, &image_id);
  if (r < 0) {
    lderr(cct) << "error reading image id object: "
	       << cpp_strerror(-r) << dendl;
    return r;
  }

  string image_header_oid = util::header_name(image_id);

  ldout(cct, 20) << "removing image " << image_name
		 << " image id " << image_header_oid << dendl;

  cls::rbd::GroupSpec group_spec(group_id, group_ioctx.get_id());

  cls::rbd::GroupImageStatus incomplete_st(image_id, image_ioctx.get_id(),
				cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE);

  cls::rbd::GroupImageSpec spec(image_id, image_ioctx.get_id());

  r = cls_client::group_image_set(&group_ioctx, group_header_oid,
				  incomplete_st);

  if (r < 0) {
    lderr(cct) << "couldn't put image into removing state: "
	       << cpp_strerror(-r) << dendl;
    return r;
  }

  r = cls_client::image_remove_group(&image_ioctx, image_header_oid,
				     group_spec);
  if ((r < 0) && (r != -ENOENT)) {
    lderr(cct) << "couldn't remove group reference from image"
	       << cpp_strerror(-r) << dendl;
    return r;
  }

  r = cls_client::group_image_remove(&group_ioctx, group_header_oid, spec);
  if (r < 0) {
    lderr(cct) << "couldn't remove image from group"
	       << cpp_strerror(-r) << dendl;
    return r;
  }

  return 0;
}

int group_image_list(librados::IoCtx& group_ioctx,
		     const char *group_name,
		     std::vector<group_image_status_t>& images)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  ldout(cct, 20) << "group_image_list " << &group_ioctx
		 << " group name " << group_name << dendl;

  string group_id;

  int r = cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY,
				 group_name, &group_id);
  if (r < 0) {
    lderr(cct) << "error reading consistency group id object: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }
  string group_header_oid = util::group_header_name(group_id);

  ldout(cct, 20) << "listing images in group name "
		 << group_name << " group id " << group_header_oid << dendl;

  std::vector<cls::rbd::GroupImageStatus> image_ids;

  const int max_read = 1024;
  do {
    std::vector<cls::rbd::GroupImageStatus> image_ids_page;
    cls::rbd::GroupImageSpec start_last;

    r = cls_client::group_image_list(&group_ioctx, group_header_oid,
	start_last, max_read, image_ids_page);

    if (r < 0) {
      lderr(cct) << "error reading image list from consistency group: "
	<< cpp_strerror(-r) << dendl;
      return r;
    }
    image_ids.insert(image_ids.end(),
		     image_ids_page.begin(), image_ids_page.end());

    if (image_ids_page.size() > 0)
      start_last = image_ids_page.rbegin()->spec;

    r = image_ids_page.size();
  } while (r == max_read);

  for (auto i : image_ids) {
    librados::Rados rados(group_ioctx);
    IoCtx ioctx;
    rados.ioctx_create2(i.spec.pool_id, ioctx);
    std::string image_name;
    r = cls_client::dir_get_name(&ioctx, RBD_DIRECTORY,
				 i.spec.image_id, &image_name);
    if (r < 0) {
      return r;
    }

    images.push_back(
	group_image_status_t {
	   image_name,
	   i.spec.pool_id,
	   static_cast<group_image_state_t>(i.state)});
  }

  return 0;
}

int image_get_group(ImageCtx *ictx, group_spec_t *group_spec)
{
  int r = ictx->state->refresh_if_required();
  if (r < 0)
    return r;

  if (-1 != ictx->group_spec.pool_id) {
    librados::Rados rados(ictx->md_ctx);
    IoCtx ioctx;
    rados.ioctx_create2(ictx->group_spec.pool_id, ioctx);

    std::string group_name;
    r = cls_client::dir_get_name(&ioctx, RBD_GROUP_DIRECTORY,
				 ictx->group_spec.group_id, &group_name);
    if (r < 0)
      return r;
    group_spec->pool = ictx->group_spec.pool_id;
    group_spec->name = group_name;
  } else {
    group_spec->pool = -1;
    group_spec->name = "";
  }

  return 0;
}

int group_snap_name_check_duplicate(librados::IoCtx& group_ioctx,
				    const char *group_name,
				    const char *snap_name)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();

  std::vector<group_snap_spec_t> snaps;
  int r = group_snap_list(group_ioctx, group_name, snaps);
  if (r < 0) {
    lderr(cct) << "failed to list existing snapshots while checking name duplicates: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }
  for (auto i: snaps) {
    if (i.name == string(snap_name)) {
      lderr(cct) << "snapshot with this name already exists: "
		 << cpp_strerror(r)
		 << dendl;
      return -EEXIST;
    }
  }
  return 0;
}

int group_snap_create(librados::IoCtx& group_ioctx,
		      const char *group_name, const char *snap_name)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  librados::Rados rados(group_ioctx);

  int r = group_snap_name_check_duplicate(group_ioctx, group_name, snap_name);
  if (r < 0) {
    return r;
  }

  string group_id;
  uint64_t snap_seq = 0;
  cls::rbd::GroupSnapshot gs;
  vector<cls::rbd::ImageSnapshotRef> image_snaps;
  vector<string> ind_snap_names;

  r = cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY,
			     group_name, &group_id);
  if (r < 0) {
    lderr(cct) << "error reading consistency group id object: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }
  string group_header_oid = util::group_header_name(group_id);

  std::vector<group_image_status_t> images;
  r = librbd::group_image_list(group_ioctx, group_name, images);
  if (r < 0) {
    return r;
  }
  int n = images.size();
  std::vector<librbd::IoCtx*> io_ctxs;
  std::vector<librbd::ImageCtx*> ictxs;
  std::vector<C_SaferCond*> on_finishes;
  for (auto i: images) {
    librbd::IoCtx* image_io_ctx = new librbd::IoCtx;

    r = rados.ioctx_create2(i.pool, *image_io_ctx);
    if (r < 0) {
      ldout(cct, 1) << "Failed to create io context for image" << dendl;
    }

    librbd::ImageCtx* image_ctx = new ImageCtx(i.name.c_str(), "", "",
					       *image_io_ctx, false);

    C_SaferCond* on_finish = new C_SaferCond;

    image_ctx->state->open(on_finish);

    ictxs.push_back(image_ctx);
    on_finishes.push_back(on_finish);
  }
  int ret_code = 0;
  for (int i = 0; i < n; ++i) {
    r = on_finishes[i]->wait();
    if (r < 0) {
      delete ictxs[i];
      ictxs[i] = nullptr;
      ret_code = r;
      delete on_finishes[i];
    }
  }
  if (ret_code != 0) {
    goto cleanup;
  }
  for (auto i: ictxs) {
    i->exclusive_lock->block_requests(-EBUSY);
  }
  for (int i = 0; i < n; ++i) {
    ImageCtx *ictx = ictxs[i];
    RWLock::RLocker owner_lock(ictx->owner_lock);

    ictx->exclusive_lock->request_lock(on_finishes[i]);
  }

  ret_code = 0;
  for (auto i: on_finishes) {
    r = i->wait();
    if (r < 0) {
      ret_code = r;
    }
  }
  if (ret_code != 0) {
    goto cleanup;
  }

  r = cls_client::group_snap_next_seq(&group_ioctx, group_header_oid, &snap_seq);
  if (r < 0) {
    ret_code = r;
    goto cleanup;
  }

  gs.id = snap_seq;
  gs.uuid = "";
  gs.name = string(snap_name);
  gs.state = cls::rbd::GROUP_SNAPSHOT_STATE_PENDING;

  r = cls_client::group_snap_save(&group_ioctx, group_header_oid, gs);
  if (r < 0) {
    ret_code = r;
    goto cleanup;
  }

  image_snaps = vector<cls::rbd::ImageSnapshotRef>(n, cls::rbd::ImageSnapshotRef());

  for (int i = 0; i < n; ++i) {
    ImageCtx *ictx = ictxs[i];
    cls::rbd::SnapshotNamespace ne =
			 cls::rbd::GroupSnapshotNamespace(group_ioctx.get_id(),
							  group_id,
							  snap_seq);

    C_SaferCond* on_finish = new C_SaferCond;

    std::stringstream ind_snap_name;
    ind_snap_name << snap_name << "_" << group_id << "_" << snap_seq;
    ind_snap_names.push_back(ind_snap_name.str());
    ictx->operations->snap_create(ind_snap_name.str().c_str(), ne, on_finish);

    on_finishes[i] = on_finish;
  }

  ret_code = 0;
  for (int i = 0; i < n; ++i) {
    r = on_finishes[i]->wait();
    if (r < 0) {
      ret_code = r;
    } else {
      ImageCtx *ictx = ictxs[i];
      ldout(cct, 1) << "Get snap id with name " << ind_snap_names[i] << dendl;
      ictx->snap_lock.get_read();
      snap_t snap_id = ictx->get_snap_id(ind_snap_names[i]);
      ictx->snap_lock.put_read();
      image_snaps[i].snap_id = snapid_t(snap_id);
      image_snaps[i].pool = ictx->data_ctx.get_id();
      image_snaps[i].image_id = ictx->id;
    }
  }
  if (ret_code != 0) {
    goto cleanup;
  }

  gs.snaps = image_snaps;
  gs.state = cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE;

  r = cls_client::group_snap_save(&group_ioctx, group_header_oid, gs);
  if (r < 0) {
    ret_code = r;
    goto cleanup;
  }

cleanup:
  for (int i = 0; i < n; ++i) {
    if (ictxs[i] != nullptr) {
      ictxs[i]->state->close();
    }
    delete on_finishes[i];
  }
  return ret_code;
}

int group_snap_list_cls(librados::IoCtx& group_ioctx, const char *group_name,
		        std::vector<cls::rbd::GroupSnapshot>& cls_snaps)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  librados::Rados rados(group_ioctx);

  string group_id;
  cls::rbd::GroupSnapshot gs;
  vector<string> ind_snap_names;

  int r = cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY,
				 group_name, &group_id);
  if (r < 0) {
    lderr(cct) << "error reading consistency group id object: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }
  string group_header_oid = util::group_header_name(group_id);

  const int max_read = 1024;

  do {
    cls::rbd::GroupSnapshot snap_last;
    vector<cls::rbd::GroupSnapshot> snaps_page;

    r = cls_client::group_snap_list(&group_ioctx, group_header_oid,
				    snap_last, max_read, snaps_page);

    if (r < 0) {
      lderr(cct) << "error reading snap list from consistency group: "
	<< cpp_strerror(-r) << dendl;
      return r;
    }
    cls_snaps.insert(cls_snaps.end(), snaps_page.begin(), snaps_page.end());

  } while (r == max_read);

  return 0;
}

int group_snap_remove(librados::IoCtx& group_ioctx, const char *group_name,
		      const char *snap_name)
{
  CephContext *cct = (CephContext *)group_ioctx.cct();
  librados::Rados rados(group_ioctx);

  std::vector<cls::rbd::GroupSnapshot> snaps;
  std::vector<C_SaferCond*> on_finishes;
  int ret_code = 0;

  string group_id;
  int r = cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY,
				 group_name, &group_id);
  if (r < 0) {
    lderr(cct) << "error reading consistency group id object: "
	       << cpp_strerror(r)
	       << dendl;
    return r;
  }
  string group_header_oid = util::group_header_name(group_id);

  r = group_snap_list_cls(group_ioctx, group_name, snaps);
  if (r < 0) {
    return r;
  }
  cls::rbd::GroupSnapshot *gs = nullptr;
  int n = snaps.size();
  for (int i = 0; i < n; ++i) {
    if (snaps[i].name == string(snap_name)) {
      gs = &snaps[i];
    }
  }
  if (gs == nullptr) {
    return -ENOENT;
  }

  std::vector<librbd::IoCtx*> io_ctxs;
  std::vector<librbd::ImageCtx*> ictxs;
  if (gs->state == cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
    n = gs->snaps.size();

    for (int i = 0; i < n; ++i) {
      librbd::IoCtx* image_io_ctx = new librbd::IoCtx;
      r = rados.ioctx_create2(gs->snaps[i].pool, *image_io_ctx);
      if (r < 0) {
	ldout(cct, 1) << "Failed to create io context for image" << dendl;
      }

      std::string image_name;
      r = cls_client::dir_get_name(image_io_ctx, RBD_DIRECTORY,
			       gs->snaps[i].image_id, &image_name);
      if (r < 0) {
	goto cleanup;
      }

      librbd::ImageCtx* image_ctx = new ImageCtx(image_name, "", "",
						 *image_io_ctx, false);

      C_SaferCond* on_finish = new C_SaferCond;

      image_ctx->state->open(on_finish);

      ictxs.push_back(image_ctx);
      on_finishes.push_back(on_finish);
    }

    int ret_code = 0;
    for (int i = 0; i < n; ++i) {
      r = on_finishes[i]->wait();
      if (r < 0) {
	delete ictxs[i];
	ictxs[i] = nullptr;
	ret_code = r;
	delete on_finishes[i];
      }
      if (ictxs[i]->id != gs->snaps[i].image_id) {
	ldout(cct, 1) << "An image was renamed during snapshot removal. Try again later." << dendl;
	ret_code = -EAGAIN;
      }
    }
    if (ret_code != 0) {
      goto cleanup;
    }

    for (int i = 0; i < n; ++i) {
      ImageCtx *ictx = ictxs[i];
      std::string snap_name;
      RWLock::RLocker l(ictx->snap_lock);
      r = ictx->get_snap_name(gs->snaps[i].snap_id, &snap_name);
      if (r < 0) {
	goto cleanup;
      }
      ictx->operations->snap_remove(snap_name.c_str(), on_finishes[i]);
    }

    for (int i = 0; i < n; ++i) {
      r = on_finishes[i]->wait();
      if (r < 0 && r != -ENOENT) { // if previous attempts to remove this snapshot failed then the image's snapshot may not exist
	ret_code = r;
	delete on_finishes[i];
      }
    }

    if (ret_code != 0) {
      goto cleanup;
    }

    r = cls_client::group_snap_remove(&group_ioctx, group_header_oid, gs->id);
    if (r < 0) {
      ret_code = r;
      goto cleanup;
    }
  }

cleanup:
  for (int i = 0; i < n; ++i) {
    if (ictxs[i] != nullptr) {
      ictxs[i]->state->close();
    }
    delete on_finishes[i];
  }
  return ret_code;
}

int group_snap_list(librados::IoCtx& group_ioctx, const char *group_name,
		     std::vector<group_snap_spec_t>& snaps)
{
  std::vector<cls::rbd::GroupSnapshot> cls_snaps;

  int r = group_snap_list_cls(group_ioctx, group_name, cls_snaps);
  if (r < 0) {
    return r;
  }

  for (auto i : cls_snaps) {
    snaps.push_back(
	group_snap_spec_t {
	   i.name,
	   static_cast<group_snap_state_t>(i.state)});

  }
  return 0;
}


} // namespace librbd
