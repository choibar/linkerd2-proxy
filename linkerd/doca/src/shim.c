#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <doca_log.h>
#include <doca_aes_gcm.h>
#include <doca_comch_consumer.h>
#include <doca_dev.h>
#include <doca_dma.h>
#include <doca_dpa.h>
#include <doca_error.h>
#include <doca_pe.h>
#include <doca_sha.h>
#include <doca_version.h>

#include "buffer.h"
#include "common.h"
#include "comch_consumer.h"
#include "comch_msgq.h"
#include "comch_server.h"
#include "dma.h"
#include "object.h"
#include "dpa.h"
#include "dpa_common.h"
#include "ring.h"

struct dmesh_doca_probe_report {
	uint32_t device_count;
	int32_t devinfo_status;
	int32_t open_status;
	int32_t close_status;
	int32_t dma_status;
	int32_t aes_gcm_status;
	int32_t sha_status;
	int32_t dpa_status;
	char compile_version[32];
	char runtime_version[32];
	char first_device_pci[64];
};

// static void cleanup_comch(struct dmesh_doca_comch *handle)
// {
// 	if (handle == NULL)
// 		return;

// 	if (handle->objs.consumer != NULL)
// 		(void)doca_ctx_stop(doca_comch_consumer_as_ctx(handle->objs.consumer));

// 	while (handle->objs.consumer != NULL && handle->objs.consumer_pe != NULL) {
// 		enum doca_ctx_states state;
// 		doca_error_t result;

// 		(void)doca_pe_progress(handle->objs.consumer_pe);
// 		result = doca_ctx_get_state(doca_comch_consumer_as_ctx(handle->objs.consumer), &state);
// 		if (result != DOCA_SUCCESS || state == DOCA_CTX_STATE_IDLE)
// 			break;
// 	}

// 	clean_comch_consumer(handle->objs.consumer, handle->objs.consumer_pe);
// 	handle->objs.consumer = NULL;
// 	handle->objs.consumer_pe = NULL;

// 	if (handle->objs.consumer_mem != NULL) {
// 		clean_local_mem_bufs(handle->objs.consumer_mem);
// 		free(handle->objs.consumer_mem);
// 		handle->objs.consumer_mem = NULL;
// 	}

// 	cleanup_objects(&handle->objs);
// }

static void copy_str(char *dst, size_t dst_len, const char *src)
{
	if (dst == NULL || dst_len == 0)
		return;

	if (src == NULL)
		src = "";

	snprintf(dst, dst_len, "%s", src);
}

static void init_report(struct dmesh_doca_probe_report *report)
{
	memset(report, 0, sizeof(*report));
	report->devinfo_status = DOCA_ERROR_UNEXPECTED;
	report->open_status = DOCA_ERROR_NOT_FOUND;
	report->close_status = DOCA_SUCCESS;
	report->dma_status = DOCA_ERROR_NOT_FOUND;
	report->aes_gcm_status = DOCA_ERROR_NOT_FOUND;
	report->sha_status = DOCA_ERROR_NOT_FOUND;
	report->dpa_status = DOCA_ERROR_NOT_FOUND;
	copy_str(report->compile_version, sizeof(report->compile_version), doca_version());
	copy_str(report->runtime_version, sizeof(report->runtime_version), doca_version_runtime());
}

int32_t dmesh_doca_probe(struct dmesh_doca_probe_report *report)
{
	struct doca_devinfo **dev_list = NULL;
	struct doca_dev *dev = NULL;
	struct doca_dma *dma = NULL;
	struct doca_aes_gcm *aes_gcm = NULL;
	struct doca_sha *sha = NULL;
	struct doca_dpa *dpa = NULL;
	uint32_t nb_devs = 0;
	doca_error_t result;

	if (report == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	init_report(report);

	result = doca_devinfo_create_list(&dev_list, &nb_devs);
	report->devinfo_status = result;
	report->device_count = nb_devs;
	if (result != DOCA_SUCCESS)
		return result;

	if (nb_devs == 0) {
		doca_devinfo_destroy_list(dev_list);
		return DOCA_ERROR_NOT_FOUND;
	}

	result = doca_devinfo_get_pci_addr_str(dev_list[0], report->first_device_pci);
	if (result != DOCA_SUCCESS)
		copy_str(report->first_device_pci, sizeof(report->first_device_pci), "<unknown>");

	result = doca_dev_open(dev_list[0], &dev);
	report->open_status = result;
	if (result != DOCA_SUCCESS) {
		doca_devinfo_destroy_list(dev_list);
		return result;
	}

	report->dma_status = doca_dma_create(dev, &dma);
	if (report->dma_status == DOCA_SUCCESS)
		doca_dma_destroy(dma);

	report->aes_gcm_status = doca_aes_gcm_create(dev, &aes_gcm);
	if (report->aes_gcm_status == DOCA_SUCCESS)
		doca_aes_gcm_destroy(aes_gcm);

	report->sha_status = doca_sha_create(dev, &sha);
	if (report->sha_status == DOCA_SUCCESS)
		doca_sha_destroy(sha);

	report->dpa_status = doca_dpa_create(dev, &dpa);
	if (report->dpa_status == DOCA_SUCCESS)
		doca_dpa_destroy(dpa);

	report->close_status = doca_dev_close(dev);
	doca_devinfo_destroy_list(dev_list);

	return DOCA_SUCCESS;
}

const char *dmesh_doca_error_name(int32_t error)
{
	return doca_error_get_name((doca_error_t)error);
}

const char *dmesh_doca_error_descr(int32_t error)
{
	return doca_error_get_descr((doca_error_t)error);
}

/*
 * ---------------------------------------------------------------------------
 * Data-path (consumer PE) accessors for the Rust AsyncFd driver.
 *
 * The consumer PE is created by the shared-infrastructure step of
 * dmesh_doca_ctrl_advance(), so these return DOCA_ERROR_BAD_STATE until the
 * first successful advance() call.
 * ---------------------------------------------------------------------------
 */

int32_t dmesh_doca_data_get_fd(struct objects *objs, int *out_fd)
{
	doca_notification_handle_t handle;
	doca_error_t result;

	if (objs == NULL || out_fd == NULL)
		return DOCA_ERROR_INVALID_VALUE;
	if (objs->consumer_pe == NULL)
		return DOCA_ERROR_BAD_STATE;

	result = doca_pe_get_notification_handle(objs->consumer_pe, &handle);
	if (result != DOCA_SUCCESS)
		return result;

	*out_fd = (int)handle;
	return DOCA_SUCCESS;
}

int32_t dmesh_doca_data_arm(struct objects *objs)
{
	if (objs == NULL || objs->consumer_pe == NULL)
		return DOCA_ERROR_BAD_STATE;

	return doca_pe_request_notification(objs->consumer_pe);
}

/* Clear the notification and progress the consumer PE up to `budget` events.
 * Returns the number of events processed via *out_drained (a full budget means
 * more work is pending and the caller should not sleep). */
int32_t dmesh_doca_data_clear_and_drain(struct objects *objs, int fd, int budget, int *out_drained)
{
	doca_error_t result;
	int drained = 0;

	if (objs == NULL || objs->consumer_pe == NULL || out_drained == NULL)
		return DOCA_ERROR_BAD_STATE;

	result = doca_pe_clear_notification(objs->consumer_pe, (doca_notification_handle_t)fd);
	if (result != DOCA_SUCCESS)
		return result;

	while (drained < budget && doca_pe_progress(objs->consumer_pe) != 0)
		drained++;

	*out_drained = drained;
	return DOCA_SUCCESS;
}

/* Progress the consumer PE up to `budget` events WITHOUT touching the
 * notification handle. Used after the first conn teardown on this worker:
 * teardown corrupts libdoca's notification bookkeeping for this PE (a later
 * doca_pe_clear_notification hits a NULL internal pointer - reproduced and
 * bisected; even with every teardown-side destroy deferred/leaked the crash
 * persists, so it is triggered by the ctx stop path itself). The driver then
 * runs this PE on its 1ms safety-net tick instead of arm/clear. */
int32_t dmesh_doca_data_drain_only(struct objects *objs, int budget, int *out_drained)
{
	int drained = 0;

	if (objs == NULL || objs->consumer_pe == NULL || out_drained == NULL)
		return DOCA_ERROR_BAD_STATE;

	while (drained < budget && doca_pe_progress(objs->consumer_pe) != 0)
		drained++;

	*out_drained = drained;
	return DOCA_SUCCESS;
}

/*
 * ---------------------------------------------------------------------------
 * Connection registry / stats introspection.
 * ---------------------------------------------------------------------------
 */


/* Teardown grave reaping is retired: conn teardown destroys everything
 * inline again (see comch_server.c). Kept as a no-op so the driver's call
 * site stays source-compatible. */
void dmesh_doca_reap_graves(struct objects *objs)
{
	(void)objs;
}

int32_t dmesh_doca_max_conns(void)
{
	return DMESH_MAX_CONNECTIONS;
}

/* Copy the flow identity (4-tuple + source workload) of a slot. The values
 * are meaningful once the slot's metadata message arrived (state >= 2). */
int32_t dmesh_doca_conn_flow_get(struct objects *objs, int32_t slot,
				 uint32_t *src_ip, uint16_t *src_port,
				 uint32_t *dst_ip, uint16_t *dst_port,
				 char *workload, int32_t workload_len)
{
	struct dmesh_flow_id *flow;

	if (objs == NULL || slot < 0 || slot >= DMESH_MAX_CONNECTIONS)
		return DOCA_ERROR_INVALID_VALUE;

	flow = &objs->conns[slot].flow;
	if (src_ip != NULL)
		*src_ip = flow->src_ip;
	if (src_port != NULL)
		*src_port = flow->src_port;
	if (dst_ip != NULL)
		*dst_ip = flow->dst_ip;
	if (dst_port != NULL)
		*dst_port = flow->dst_port;
	if (workload != NULL && workload_len > 0)
		snprintf(workload, (size_t)workload_len, "%s", flow->src_workload);
	return DOCA_SUCCESS;
}

/* Returns the enum dmesh_conn_state value of a slot, or -1 on bad input */
int32_t dmesh_doca_conn_state_get(struct objects *objs, int32_t slot)
{
	if (objs == NULL || slot < 0 || slot >= DMESH_MAX_CONNECTIONS)
		return -1;

	return (int32_t)objs->conns[slot].state;
}

/* Called only after the Rust IO mutex has excluded all staging readers/writers. */
int32_t dmesh_doca_conn_readers_detached(struct objects *objs, int32_t slot)
{
	return (int32_t)dmesh_flow_readers_detached(objs, slot);
}

void dmesh_doca_stats_get(struct objects *objs,
			  int64_t *sent, int64_t *recv, int64_t *recv_bytes,
			  int64_t *dma_pending, int64_t *dma_dropped)
{
	int64_t pending = 0, dropped = 0;
	int i;

	if (objs == NULL)
		return;

	for (i = 0; i < DMESH_MAX_CONNECTIONS; i++) {
		pending += objs->conns[i].dma_pending_cnt;
		dropped += objs->conns[i].dma_dropped_copies;
	}

	if (sent != NULL)
		*sent = (int64_t)objs->sent_msg_cnt;
	if (recv != NULL)
		*recv = (int64_t)objs->recv_msg_cnt;
	if (recv_bytes != NULL)
		*recv_bytes = (int64_t)objs->recv_bytes;
	if (dma_pending != NULL)
		*dma_pending = pending;
	if (dma_dropped != NULL)
		*dma_dropped = dropped;
}

/*
 * ---------------------------------------------------------------------------
 * Zero-copy recv: expose the staging buffer + completed-segment queue so the
 * Rust DmeshIo can read DMA'd bytes directly out of the mapped staging region,
 * without any extra copy on the DPU.
 * ---------------------------------------------------------------------------
 */

/* Base pointer + length of a connection's staging buffer (where the DPA DMAs
 * host bytes). Valid once the connection reached DMESH_CONN_RUNNING. */
int32_t dmesh_doca_conn_staging_base(struct objects *objs, int32_t slot,
				     const uint8_t **out_base, size_t *out_len)
{
	struct dmesh_conn *conn;

	if (objs == NULL || slot < 0 || slot >= DMESH_MAX_CONNECTIONS ||
	    out_base == NULL || out_len == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	conn = &objs->conns[slot];
	if (conn->dma_buffer == NULL || conn->local_mmap == NULL)
		return DOCA_ERROR_BAD_STATE;

	*out_base = (const uint8_t *)conn->dma_buffer;
	*out_len = BUFFER_SIZE;
	return DOCA_SUCCESS;
}

/* Pop the next completed recv segment for a slot into (*out_pos, *out_len).
 * Returns DOCA_SUCCESS with a segment, or DOCA_ERROR_EMPTY when none pending.
 * Single-consumer: only the driver task calls this. */
int32_t dmesh_doca_conn_recv_pop(struct objects *objs, int32_t slot,
				 uint32_t *out_pos, uint32_t *out_len)
{
	struct dmesh_conn *conn;
	struct dmesh_recv_seg *seg;

	if (objs == NULL || slot < 0 || slot >= DMESH_MAX_CONNECTIONS ||
	    out_pos == NULL || out_len == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	conn = &objs->conns[slot];
	if (conn->recv_seg_cnt == 0)
		return DOCA_ERROR_EMPTY;

	seg = &conn->recv_segs[conn->recv_seg_head];
	*out_pos = seg->pos;
	*out_len = seg->len;
	conn->recv_seg_head = (conn->recv_seg_head + 1) % DMESH_RECV_SEG_MAX;
	conn->recv_seg_cnt--;
	return DOCA_SUCCESS;
}

/* Release a segment after the reader consumed its bytes. TODO(flow-control):
 * this is where a staging-buffer read watermark would be published back to the
 * DPA to gate reuse; today the staging ring has no producer backpressure, so
 * this only advances an accounting counter. */
int32_t dmesh_doca_conn_recv_release(struct objects *objs, int32_t slot,
				     uint32_t pos, uint32_t len)
{
	(void)pos;
	(void)len;

	if (objs == NULL || slot < 0 || slot >= DMESH_MAX_CONNECTIONS)
		return DOCA_ERROR_INVALID_VALUE;

	return DOCA_SUCCESS;
}

/* Publish the reader's staging watermark to this slot's DPA thread
 * (dpa_thread_arg.rd_pos) so the kernel's staging gate can advance. Called
 * from the driver tick only when the watermark moved; one small h2d_memcpy. */
int32_t dmesh_doca_conn_rx_watermark(struct objects *objs, int32_t slot, uint32_t pos)
{
	struct dmesh_conn *conn;
	struct dmesh_doca_dpa_thread *t;

	if (objs == NULL || slot < 0 || slot >= DMESH_MAX_CONNECTIONS)
		return DOCA_ERROR_INVALID_VALUE;
	conn = &objs->conns[slot];
	t = conn->dpa_thread;
	if (t == NULL || t->thread == NULL || t->arg == 0)
		return DOCA_ERROR_BAD_STATE;
	return doca_dpa_h2d_memcpy(t->dpa, t->arg + offsetof(struct dpa_thread_arg, rd_pos),
				   &pos, sizeof(pos));
}

/* doca_dpa_dev_comch_producer_dma_copy (the fused copy+notify the host reverse
 * DPA runs) fires a completion only when the copy is a multiple of 128B, or a
 * single sub-block <=128B. Emit the largest 128-aligned copy (<=8064 = 63*128,
 * under the 8KB single-DMA limit); the final <=128B tail is a valid sub-block.
 * The host reassembles the response byte stream from the segments. */
#define DMESH_TX_DMA_MAXMUL 8064

/* Reverse (response) path: queue outbound bytes for DMA back to the host's
 * rcvbuf. Copies data into this connection's tx_staging and appends
 * descriptor(s) to its rcv_ring; the host's DPA thread polls the ring and
 * performs the DPU->host DMA. Returns the number of bytes accepted (may be < len
 * when the ring is momentarily full - the driver retries the remainder), or a
 * negative doca_error_t. Single-producer: only the driver task calls this.
 *
 * TODO(flow-control): tx_staging has no host-consumption backpressure yet, so a
 * response larger than tx_staging_len could wrap and overwrite bytes the host
 * has not DMA'd. Fine for the current sub-buffer responses; a staging watermark
 * (mirror of the recv path) is the general fix. */
/* Flow mode of a slot: 0 = client flow, 1 = backend provider (see
 * DMESH_FLOW_MODE_*). Negative doca_error_t on bad args. */
int32_t dmesh_doca_conn_mode_get(struct objects *objs, int32_t slot)
{
	if (objs == NULL || slot < 0 || slot >= DMESH_MAX_CONNECTIONS)
		return -(int32_t)DOCA_ERROR_INVALID_VALUE;
	/* BACKEND_PULL is a backend flow whose reverse path the host's DPA pulls
	 * (the host library's host-dpa reverse path); the role is what callers switch on, the
	 * wire is the datapath's business (DMESH_FLOW_USES_PUSH on flow.mode). */
	if (objs->conns[slot].flow.mode == DMESH_FLOW_MODE_BACKEND_PULL)
		return (int32_t)DMESH_FLOW_MODE_BACKEND;
	return (int32_t)objs->conns[slot].flow.mode;
}

/* Report the connection's mapped tx_staging region so the Rust writer can copy
 * response bytes straight into it (write-side zero-copy). Usable length
 * reserves a 64B tail for the backend push descriptor shadow (harmless to
 * reserve on client channels too, keeping one rule). Negative on bad state. */
int32_t dmesh_doca_conn_tx_staging(struct objects *objs, int32_t slot,
				   uintptr_t *out_base, size_t *out_len)
{
	struct dmesh_conn *conn;

	if (objs == NULL || slot < 0 || slot >= DMESH_MAX_CONNECTIONS ||
	    out_base == NULL || out_len == NULL)
		return -(int32_t)DOCA_ERROR_INVALID_VALUE;
	conn = &objs->conns[slot];
	if (!conn->reverse_exported || conn->tx_staging == NULL || conn->tx_staging_len <= 64)
		return -(int32_t)DOCA_ERROR_BAD_STATE;   /* reverse path not ready yet */

	*out_base = (uintptr_t)conn->tx_staging;
	*out_len = conn->tx_staging_len - 64;
	return 0;
}

/* Publish response bytes the Rust side already wrote into tx_staging at
 * [pos, pos+len) - NO memcpy. Returns bytes accepted (may be < len if the DMA
 * ring/backend batch is momentarily full; the driver retries), or a negative
 * doca_error_t. The range never crosses the staging wrap (the writer splits at
 * the boundary and calls again). */
int32_t dmesh_doca_conn_send_staged(struct objects *objs, int32_t slot,
				    uint32_t pos, uint32_t len)
{
	struct dmesh_conn *conn;
	size_t sent = 0;

	if (objs == NULL || slot < 0 || slot >= DMESH_MAX_CONNECTIONS)
		return -(int32_t)DOCA_ERROR_INVALID_VALUE;

	conn = &objs->conns[slot];

	/* Backend channels (안 2): push with this connection's doca_dma engine -
	 * no rcv_ring, no host DPA. One <=8KB batch outstanding; the driver
	 * retries the remainder on later ticks. */
	if (DMESH_FLOW_USES_PUSH(conn->flow.mode)) {
		if (!conn->reverse_exported || conn->tx_staging == NULL)
			return -(int32_t)DOCA_ERROR_BAD_STATE;
		while (sent < len) {
			int r = dmesh_dma_push_staged(conn, pos + (uint32_t)sent,
						      len - (uint32_t)sent);

			if (r < 0)
				return sent > 0 ? (int32_t)sent : (int32_t)r;
			if (r == 0)
				break;          /* batch in flight; retry later */
			sent += (size_t)r;
		}
		return (int32_t)sent;
	}

	if (!conn->reverse_exported || conn->tx_staging == NULL || conn->rcv_ring == NULL)
		return -(int32_t)DOCA_ERROR_BAD_STATE;   /* reverse path not ready yet */

	while (sent < len) {
		size_t remaining = len - sent;
		size_t chunk;
		struct dma_desc *desc;

		/* 128-align the descriptor (see DMESH_TX_DMA_MAXMUL): largest
		 * multiple of 128 up to 8064, or a single <=128B sub-block for the
		 * tail. The bytes are already in staging; we only emit descriptors. */
		if (remaining <= 128)
			chunk = remaining;
		else if (remaining >= DMESH_TX_DMA_MAXMUL)
			chunk = DMESH_TX_DMA_MAXMUL;
		else
			chunk = remaining & ~(size_t)127;

		/* Non-blocking ring check: report a partial send rather than
		 * busy-waiting in the driver thread if the host is behind. */
		if (conn->rcv_ring->head - conn->rcv_ring->ctrl->consumer_head >= conn->rcv_ring->size)
			break;

		desc = get_next_dma_desc(conn->rcv_ring);
		desc->mmap = 0;   /* kernel uses thread_arg->host_mmap as the source */
		desc->addr = (uint64_t)conn->tx_staging + pos + sent;
		desc->size = chunk;
		commit_dma_desc(conn->rcv_ring);

		sent += chunk;
	}

	return (int32_t)sent;
}

int32_t dmesh_doca_init(const char *dev_pci_addr,
						  const char *rep_pci_addr,
						  const char *server_name,
						  struct objects **handle)
{
	dmesh_staging_fc = 1; /* we publish rd_pos (dmesh_doca_conn_rx_watermark) */

	struct doca_log_backend *sdk_log;
	struct objects *objs;
	doca_error_t result;

	fprintf(stderr, "[DMesh] Initializing DOCA objects with dev_pci_addr=%s, rep_pci_addr=%s, server_name=%s\n",
		dev_pci_addr, rep_pci_addr, server_name ? server_name : "<default>");

	if (dev_pci_addr == NULL || rep_pci_addr == NULL || handle == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	if (server_name == NULL)
		server_name = "DPUMesh";

	*handle = NULL;

	/* register logger backends once per process (N-driver mode calls
	 * dmesh_doca_init once per worker) */
	static bool log_inited = false;
	if (!log_inited) {
		result = doca_log_backend_create_standard();
		if (result != DOCA_SUCCESS) {
			fprintf(stderr, "Failed to create standard log backend: %s\n", doca_error_get_name(result));
			return result;
		}
		result = doca_log_backend_create_with_file_sdk(stderr, &sdk_log);
		if (result != DOCA_SUCCESS) {
			fprintf(stderr, "Failed to create log backend for SDK: %s\n", doca_error_get_name(result));
			return result;
		}
		result = doca_log_backend_set_sdk_level(sdk_log, DOCA_LOG_LEVEL_WARNING);
		if (result != DOCA_SUCCESS) {
			fprintf(stderr, "Failed to set log level for SDK log backend: %s\n", doca_error_get_name(result));
			return result;
		}
		log_inited = true;
	}
	
	objs = calloc(1, sizeof(*objs));
	if (objs == NULL)
		return DOCA_ERROR_NO_MEMORY;
	/* Rust IO may be read by other runtimes; C close must await its ACK. */
	objs->external_readers = true;

	result = open_doca_device_with_pci(dev_pci_addr, NULL, &objs->dev);
	if (result != DOCA_SUCCESS)
		goto free_handle;

	fprintf(stderr, "Opened DOCA device %s\n", dev_pci_addr);

	result = open_doca_device_rep_with_pci(objs->dev,
					       DOCA_DEVINFO_REP_FILTER_NET,
					       rep_pci_addr,
					       &objs->rep_dev);
	if (result != DOCA_SUCCESS)
		goto cleanup_handle;

	fprintf(stderr, "Opened DOCA device rep %s\n", rep_pci_addr);

	/*
	 * Start the comch control-path server WITHOUT blocking on the host
	 * connection. start_comch_ctrl_path_server creates objs->pe itself and
	 * leaves objs->phase = SERVER_STARTED. The remaining worker init (consumer,
	 * DPA, mmap waits) is driven on-demand via the dmesh_doca_ctrl_* helpers
	 * (get_fd/arm/drain/clear_and_drain/advance), which the caller runs against
	 * the control PE notification fd.
	 */
	result = start_comch_ctrl_path_server(server_name, objs, true);
	if (result != DOCA_SUCCESS)
		goto cleanup_handle;

	fprintf(stderr, "Started DOCA comch server %s (awaiting host connection)\n", server_name);

	*handle = objs;
	return DOCA_SUCCESS;

cleanup_handle:
	cleanup_objects(objs);
free_handle:
	free(objs);
	return result;
}

void dmesh_doca_comch_destroy(struct objects *handle)
{
	if (handle == NULL)
		return;

	/* A dropped driver cannot prove outstanding Rust references or failed DMA
	 * quiescence have retired. Retain the whole device domain on this void API. */
	if (dmesh_objects_have_live_flows(handle)) {
		fprintf(stderr, "[DMesh] retaining live flow resources after driver drop\n");
		return;
	}
	for (int i = 0; i < DMESH_MAX_SESSIONS; ++i) {
		if (handle->sessions[i].occupied || handle->sessions[i].sends_pending) {
			fprintf(stderr, "[DMesh] retaining control session resources after driver drop\n");
			return;
		}
	}
	/* NOTE: partial teardown. cleanup_objects only releases cc_server/pe/
	 * rep_dev/dev; consumer/DPA/mmap/buf_arr resources are not yet freed
	 * (a full teardown is still TODO). */
	cleanup_objects(handle);
	free(handle);
}
