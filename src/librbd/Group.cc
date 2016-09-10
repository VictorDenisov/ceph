#include "include/int_types.h"

#include <errno.h>
#include <limits.h>

#include "include/types.h"
#include "include/uuid.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Throttle.h"
#include "common/event_socket.h"
#include "cls/lock/cls_lock_client.h"
#include "include/stringify.h"

#include "cls/rbd/cls_rbd.h"
#include "cls/rbd/cls_rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/journal/cls_journal_types.h"
#include "cls/journal/cls_journal_client.h"

#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequest.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/AioObjectRequest.h"
#include "librbd/image/CreateRequest.h"
#include "librbd/DiffIterate.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/journal/DisabledPolicy.h"
#include "librbd/journal/StandardPolicy.h"
#include "librbd/journal/Types.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/ObjectMap.h"
#include "librbd/Operations.h"
#include "librbd/parent_types.h"
#include "librbd/Utils.h"
#include "librbd/exclusive_lock/AutomaticPolicy.h"
#include "librbd/exclusive_lock/StandardPolicy.h"
#include "librbd/operation/TrimRequest.h"
#include "include/util.h"

#include "journal/Journaler.h"

#include <boost/scope_exit.hpp>
#include <boost/variant.hpp>
#include "include/assert.h"
#include "librbd/Group.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd: "

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

  int group_snapshot(librados::IoCtx& group_ioctx, const char *group_name,
		     const char *snap_name)
  {
    librados::Rados rados(group_ioctx);
    CephContext *cct = (CephContext *)group_ioctx.cct();
    int r = 0;
    std::vector<group_image_status_t> images_statuses;
    group_image_list(group_ioctx, group_name, images_statuses);
    for (auto image: images_statuses) {
      if (image.state != GROUP_IMAGE_STATE_ATTACHED) {
	ldout(cct, 1) << "Warning: image " << image.name
		      << " is not properly added." << dendl;

	return -1;
      }
    }

    string group_id;

    r = cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY,
			       group_name, &group_id);
    if (r < 0) {
      lderr(cct) << "error reading consistency group id object: "
                 << cpp_strerror(r)
        	 << dendl;
      return r;
    }
    string group_header_oid = util::group_header_name(group_id);

    cls_client::group_state_set(&group_ioctx, group_header_oid,
				cls::rbd::GROUP_STATE_CAPTURING_LOCK);

    librbd::RBD rbd;
    std::vector<librbd::IoCtx*> image_io_ctxs;
    std::vector<librbd::ImageCtx*> images;

    for (auto i: images_statuses) {
      librbd::IoCtx *image_io_ctx = new librbd::IoCtx;
      librbd::Image *image = new librbd::Image;

      IoCtx ioctx;
      r = rados.ioctx_create2(i.pool, *image_io_ctx);
      if (r < 0) {
	ldout(cct, 1) << "Failed to create pool" << dendl;
      }

      ImageCtx *ictx = new ImageCtx(i.name.c_str(), "", "", *image_io_ctx, false);

      r = ictx->state->open();
      if (r < 0) {
	ldout(cct, 1) << "Failed to open image" << dendl;
      }

      r = librbd::lock_acquire(ictx, RBD_LOCK_MODE_EXCLUSIVE);
      if (r < 0) {
	ldout(cct, 1) << "Failed to acquire lock" << dendl;
      }

      image_io_ctxs.push_back(image_io_ctx);
      images.push_back(ictx);
    }

    int n = image_io_ctxs.size();

    std::string s;
    std::cout << "Quiesced image. Waiting for your input" << std::endl;
    std::cin >> s;

    cls_client::group_state_set(&group_ioctx, group_header_oid,
				cls::rbd::GROUP_STATE_SNAPSHOTTING);

    r = cls_client::group_snap_candidate_create(&group_ioctx, group_header_oid,
						snap_name);

    if (r < 0)
      goto release_locks;

    cls_client::group_state_set(&group_ioctx, group_header_oid,
				cls::rbd::GROUP_STATE_MAKING_INDIVIDUAL_SNAPS);

    for (int i = 0; i < n; ++i) {
      vector<librbd::snap_info_t> snaps;

      std::cout << "Listing snapshots before" << std::endl;
      cls::rbd::PendingImageSnapshot pending_image_snapshot;
      pending_image_snapshot.pool = images_statuses[i].pool;
      pending_image_snapshot.image_id = images[i]->id;
      pending_image_snapshot.snap_name = snap_name;

      r = cls_client::group_pending_image_snap_set(&group_ioctx, group_header_oid,
						   &pending_image_snapshot);

      std::cout << "Created pending image snapshot. Waiting for your input" << std::endl;
      std::cin >> s;

      if (r < 0)
	goto release_locks;

      snap_list(images[i], snaps);
      for (int j = 0; j < snaps.size(); ++j) {
	std::cout << "id: " << snaps[j].id << std::endl;
	std::cout << "name: " << snaps[j].name << std::endl;
      }

      r = images[i]->operations->snap_create(snap_name);

      cls::rbd::ImageSnapshotRef ref;
      ref.pool = images_statuses[i].pool;
      ref.image_id = images[i]->id;
      ref.snap_id = images[i]->get_snap_id(snap_name);
      cls_client::group_snap_candidate_add(&group_ioctx, group_header_oid,
					   &ref);

      cls_client::group_state_set(&group_ioctx, group_header_oid,
				  cls::rbd::GROUP_STATE_MAKING_INDIVIDUAL_SNAPS);

      std::cout << "Snap create result: " << r << std::endl;
      std::cin >> s;

      librbd::snap_list(images[i], snaps);
      std::cout << "Listing snapshots after" << std::endl;
      for (int j = 0; j < snaps.size(); ++j) {
	std::cout << "id: " << snaps[j].id << std::endl;
	std::cout << "name: " << snaps[j].name << std::endl;
      }
    }

    cls_client::group_state_set(&group_ioctx, group_header_oid,
				cls::rbd::GROUP_STATE_COMMITTING);


    cls_client::group_snap_commit(&group_ioctx, group_header_oid);

    std::cout << "Snapshot group. Waiting for your input" << std::endl;
    std::cin >> s;

release_locks:
    cls_client::group_state_set(&group_ioctx, group_header_oid,
				cls::rbd::GROUP_STATE_RELEASING_LOCK);

    for (auto ictx : image_io_ctxs) {
      delete ictx;
    }

    for (auto img : images) {
      delete img;
    }

    cls_client::group_state_set(&group_ioctx, group_header_oid,
				cls::rbd::GROUP_STATE_NORMAL);

    return r;
  }
}
