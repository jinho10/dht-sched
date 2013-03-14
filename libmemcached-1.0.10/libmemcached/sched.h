/*
 * sched.h 
 * jinho added
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

struct ketama_stat {
    uint32_t ref;
    bool initialized_;
    uint32_t continuum_count; // server counts
    uint32_t max; // max usage
    uint32_t maxid; // max usage index
    struct sched_context *servers;

    // for statistics
    // Ketama: one of reference from one of downstream
    uint32_t continuum_points_counter; // Ketama
    struct memcached_continuum_item_st *continuum;
};

// jinho added
struct sched_dynamic {
    uint32_t ref;
    // XXX lock should be added for this because this is shared

    bool initialized_;
    bool weighted_;

    // Servers
    uint32_t continuum_count; // # servers
    struct sched_context *servers;

    // Sections
    uint32_t continuum_points_counter; // total
    struct memcached_continuum_item_st *continuum; // { u32 index, value }

    // Supplements
    time_t next_distribution_rebuild; // not used yet..
    uint32_t max; // max usage
    uint32_t maxid; // max usage index

    // sched info from user
    double alpha;
    double beta;

    // delivering info
    int removing_idx;
};

#ifdef __cplusplus
}
#endif
