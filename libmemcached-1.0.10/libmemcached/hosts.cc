/*  vim:expandtab:shiftwidth=2:tabstop=2:smarttab:
 * 
 *  Libmemcached library
 *
 *  Copyright (C) 2011 Data Differential, http://datadifferential.com/
 *  Copyright (C) 2006-2010 Brian Aker All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are
 *  met:
 *
 *      * Redistributions of source code must retain the above copyright
 *  notice, this list of conditions and the following disclaimer.
 *
 *      * Redistributions in binary form must reproduce the above
 *  copyright notice, this list of conditions and the following disclaimer
 *  in the documentation and/or other materials provided with the
 *  distribution.
 *
 *      * The names of its contributors may not be used to endorse or
 *  promote products derived from this software without specific prior
 *  written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <libmemcached/common.h>

#include <cmath>
#include <sys/time.h>

/* Protoypes (static) */
static memcached_return_t update_continuum(memcached_st *ptr);
static memcached_return_t update_dynamic(memcached_st *ptr);
double abs_double(double val);
uint32_t abs_int(int val);
uint64_t abs_int64(int64_t val);

uint32_t hs_round(uint32_t small, uint32_t big)
{
  int64_t diff = (int64_t)(big - small);
  uint32_t ret = (uint32_t)diff;

  if ( diff < 0 ) {
    ret = (uint32_t)(diff + (uint64_t)(~((uint32_t)0)));
  }

  return ret;
}

static int compare_servers(const void *p1, const void *p2)
{
  memcached_server_instance_st a= (memcached_server_instance_st)p1;
  memcached_server_instance_st b= (memcached_server_instance_st)p2;

  int return_value= strcmp(a->hostname, b->hostname);

  if (return_value == 0)
  {
    return_value= int(a->port() - b->port());
  }

  return return_value;
}

static void sort_hosts(memcached_st *ptr)
{
  if (memcached_server_count(ptr))
  {
    qsort(memcached_instance_list(ptr), memcached_server_count(ptr), sizeof(org::libmemcached::Instance), compare_servers);
  }
}


memcached_return_t run_distribution(memcached_st *ptr)
{
  if (ptr->flags.use_sort_hosts)
  {
    sort_hosts(ptr);
  }

  switch (ptr->distribution)
  {
  case MEMCACHED_DISTRIBUTION_CONSISTENT:
  case MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA:
  case MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA_SPY:
  case MEMCACHED_DISTRIBUTION_CONSISTENT_WEIGHTED:
    return update_continuum(ptr);

  // jinho added
  case MEMCACHED_DISTRIBUTION_DYNAMIC:
  case MEMCACHED_DISTRIBUTION_DYNAMIC_WEIGHTED:
    return update_dynamic(ptr);

  case MEMCACHED_DISTRIBUTION_VIRTUAL_BUCKET:
  case MEMCACHED_DISTRIBUTION_MODULA:
    break;

  case MEMCACHED_DISTRIBUTION_RANDOM:
    srandom((uint32_t) time(NULL));
    break;

  case MEMCACHED_DISTRIBUTION_CONSISTENT_MAX:
  default:
    assert_msg(0, "Invalid distribution type passed to run_distribution()");
  }

  return MEMCACHED_SUCCESS;
}

static uint32_t ketama_server_hash(const char *key, size_t key_length, uint32_t alignment)
{
  unsigned char results[16];

  libhashkit_md5_signature((unsigned char*)key, key_length, results);

  return ((uint32_t) (results[3 + alignment * 4] & 0xFF) << 24)
    | ((uint32_t) (results[2 + alignment * 4] & 0xFF) << 16)
    | ((uint32_t) (results[1 + alignment * 4] & 0xFF) << 8)
    | (results[0 + alignment * 4] & 0xFF);
}

static int continuum_item_cmp(const void *t1, const void *t2)
{
  memcached_continuum_item_st *ct1= (memcached_continuum_item_st *)t1;
  memcached_continuum_item_st *ct2= (memcached_continuum_item_st *)t2;

  /* Why 153? Hmmm... */
  WATCHPOINT_ASSERT(ct1->value != 153);
  if (ct1->value == ct2->value)
  {
    return 0;
  }
  else if (ct1->value > ct2->value)
  {
    return 1;
  }
  else
  {
    return -1;
  }
}

static memcached_return_t update_continuum(memcached_st *ptr)
{
  uint32_t continuum_index= 0;
  uint32_t pointer_counter= 0;
  uint32_t pointer_per_server= MEMCACHED_POINTS_PER_SERVER;
  uint32_t pointer_per_hash= 1;
  uint32_t live_servers= 0;
  struct timeval now;

  if (gettimeofday(&now, NULL))
  {
    return memcached_set_errno(*ptr, errno, MEMCACHED_AT);
  }

  org::libmemcached::Instance* list= memcached_instance_list(ptr);

  /* count live servers (those without a retry delay set) */
  bool is_auto_ejecting= _is_auto_eject_host(ptr);
  if (is_auto_ejecting)
  {
    live_servers= 0;
    ptr->ketama.next_distribution_rebuild= 0;
    for (uint32_t host_index= 0; host_index < memcached_server_count(ptr); ++host_index)
    {
      if (list[host_index].next_retry <= now.tv_sec)
      {
        live_servers++;
      }
      else
      {
        if (ptr->ketama.next_distribution_rebuild == 0 or list[host_index].next_retry < ptr->ketama.next_distribution_rebuild)
        {
          ptr->ketama.next_distribution_rebuild= list[host_index].next_retry;
        }
      }
    }
  }
  else
  {
    live_servers= memcached_server_count(ptr);
  }

  uint32_t points_per_server= (uint32_t) (memcached_is_weighted_ketama(ptr) ? MEMCACHED_POINTS_PER_SERVER_KETAMA : MEMCACHED_POINTS_PER_SERVER);

  if (live_servers == 0)
  {
    return MEMCACHED_SUCCESS;
  }

  if (live_servers > ptr->ketama.continuum_count)
  {
    memcached_continuum_item_st *new_ptr;

    new_ptr= libmemcached_xrealloc(ptr, ptr->ketama.continuum, (live_servers + MEMCACHED_CONTINUUM_ADDITION) * points_per_server, memcached_continuum_item_st);

    if (new_ptr == 0)
    {
      return MEMCACHED_MEMORY_ALLOCATION_FAILURE;
    }

    ptr->ketama.continuum= new_ptr;
    ptr->ketama.continuum_count= live_servers + MEMCACHED_CONTINUUM_ADDITION;

  }

  uint64_t total_weight= 0;
  if (memcached_is_weighted_ketama(ptr))
  {
    for (uint32_t host_index = 0; host_index < memcached_server_count(ptr); ++host_index)
    {
      if (is_auto_ejecting == false or list[host_index].next_retry <= now.tv_sec)
      {
        total_weight += list[host_index].weight;
      }
    }
  }

  for (uint32_t host_index= 0; host_index < memcached_server_count(ptr); ++host_index)
  {
    if (is_auto_ejecting and list[host_index].next_retry > now.tv_sec)
    {
      continue;
    }

    if (memcached_is_weighted_ketama(ptr))
    {
        float pct= (float)list[host_index].weight / (float)total_weight;
        pointer_per_server= (uint32_t) ((::floor((float) (pct * MEMCACHED_POINTS_PER_SERVER_KETAMA / 4 * (float)live_servers + 0.0000000001))) * 4);
        pointer_per_hash= 4;
        if (DEBUG)
        {
          printf("ketama_weighted:%s|%d|%llu|%u\n",
                 list[host_index].hostname,
                 list[host_index].port(),
                 (unsigned long long)list[host_index].weight,
                 pointer_per_server);
        }
    }


    if (ptr->distribution == MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA_SPY)
    {
      for (uint32_t pointer_index= 0;
           pointer_index < pointer_per_server / pointer_per_hash;
           pointer_index++)
      {
        char sort_host[1 +MEMCACHED_NI_MAXHOST +1 +MEMCACHED_NI_MAXSERV +1 + MEMCACHED_NI_MAXSERV ]= "";
        int sort_host_length;

        // Spymemcached ketema key format is: hostname/ip:port-index
        // If hostname is not available then: /ip:port-index
        sort_host_length= snprintf(sort_host, sizeof(sort_host),
                                   "/%s:%u-%u",
                                   list[host_index].hostname,
                                   (uint32_t)list[host_index].port(),
                                   pointer_index);

        if (size_t(sort_host_length) >= sizeof(sort_host) or sort_host_length < 0)
        {
          return memcached_set_error(*ptr, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT, 
                                     memcached_literal_param("snprintf(sizeof(sort_host))"));
        }

        if (DEBUG)
        {
          fprintf(stdout, "update_continuum: key is %s\n", sort_host);
        }

        if (memcached_is_weighted_ketama(ptr))
        {
          for (uint32_t x= 0; x < pointer_per_hash; x++)
          {
            uint32_t value= ketama_server_hash(sort_host, (size_t)sort_host_length, x);
            ptr->ketama.continuum[continuum_index].index= host_index;
            ptr->ketama.continuum[continuum_index++].value= value;
          }
        }
        else
        {
          uint32_t value= hashkit_digest(&ptr->hashkit, sort_host, (size_t)sort_host_length);
          ptr->ketama.continuum[continuum_index].index= host_index;
          ptr->ketama.continuum[continuum_index++].value= value;
        }
      }
    }
    else
    {
      for (uint32_t pointer_index= 1;
           pointer_index <= pointer_per_server / pointer_per_hash;
           pointer_index++)
      {
        char sort_host[MEMCACHED_NI_MAXHOST +1 +MEMCACHED_NI_MAXSERV +1 +MEMCACHED_NI_MAXSERV]= "";
        int sort_host_length;

        if (list[host_index].port() == MEMCACHED_DEFAULT_PORT)
        {
          sort_host_length= snprintf(sort_host, sizeof(sort_host),
                                     "%s-%u",
                                     list[host_index].hostname,
                                     pointer_index - 1);
        }
        else
        {
          sort_host_length= snprintf(sort_host, sizeof(sort_host),
                                     "%s:%u-%u",
                                     list[host_index].hostname,
                                     (uint32_t)list[host_index].port(),
                                     pointer_index - 1);
        }

        if (size_t(sort_host_length) >= sizeof(sort_host) or sort_host_length < 0)
        {
          return memcached_set_error(*ptr, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT, 
                                     memcached_literal_param("snprintf(sizeof(sort_host)))"));
        }

        if (memcached_is_weighted_ketama(ptr))
        {
          for (uint32_t x = 0; x < pointer_per_hash; x++)
          {
            uint32_t value= ketama_server_hash(sort_host, (size_t)sort_host_length, x);
            ptr->ketama.continuum[continuum_index].index= host_index;
            ptr->ketama.continuum[continuum_index++].value= value;
          }
        }
        else
        {
          uint32_t value= hashkit_digest(&ptr->hashkit, sort_host, (size_t)sort_host_length);
          ptr->ketama.continuum[continuum_index].index= host_index;
          ptr->ketama.continuum[continuum_index++].value= value;
        }
      }
    }

    pointer_counter+= pointer_per_server;
  }

  // jinho debugging msg for statistics -------------------
  /*
  for(uint32_t sn = 0; sn < continuum_index; sn++) {
    fprintf(stderr, "%u ", ptr->ketama.continuum[sn].index);
  }
  fprintf(stderr, "\n");
  for(uint32_t sn = 0; sn < continuum_index; sn++) {
    fprintf(stderr, "%u ", ptr->ketama.continuum[sn].value);
  }
  */
  // jinho debugging msg for statistics -------------------

  // jinho added for statistics ----------------------
  /*
  if ( ptr->ketama_stat != NULL ) {
    if ( ptr->ketama_stat->servers != NULL ) {
      libmemcached_free(NULL, ptr->ketama_stat->servers); // what is this?....hmmm
    }
  }
  */
  if ( ptr->ketama_stat != NULL ) {
    if ( ptr->ketama_stat->servers == NULL ) {
      ptr->ketama_stat->continuum_count = live_servers;
      ptr->ketama_stat->servers = (struct sched_context*)libmemcached_malloc(NULL, sizeof(struct sched_context)*live_servers);

      ptr->ketama_stat->continuum_points_counter = pointer_counter;
      ptr->ketama_stat->continuum = ptr->ketama.continuum;

      struct sched_context *servers = ptr->ketama_stat->servers;
      for (uint32_t sn = 0; sn < live_servers; sn++) {
        servers[sn].get = 0;
        servers[sn].set = 0;
        servers[sn].sum = 0;
        servers[sn].nhitrate = 0;
        servers[sn].hitrate = 0;
        servers[sn].usagerate = 0;
      }
    }
  }
  // jinho added for statistics ----------------------

  WATCHPOINT_ASSERT(ptr);
  WATCHPOINT_ASSERT(ptr->ketama.continuum);
  WATCHPOINT_ASSERT(memcached_server_count(ptr) * MEMCACHED_POINTS_PER_SERVER <= MEMCACHED_CONTINUUM_SIZE);
  ptr->ketama.continuum_points_counter= pointer_counter;
  qsort(ptr->ketama.continuum, ptr->ketama.continuum_points_counter, sizeof(memcached_continuum_item_st), continuum_item_cmp);

  if (DEBUG)
  {
    for (uint32_t pointer_index= 0; memcached_server_count(ptr) && pointer_index < ((live_servers * MEMCACHED_POINTS_PER_SERVER) - 1); pointer_index++)
    {
      WATCHPOINT_ASSERT(ptr->ketama.continuum[pointer_index].value <= ptr->ketama.continuum[pointer_index + 1].value);
    }
  }

  return MEMCACHED_SUCCESS;
}

// jinho added

//double alpha = 1;
//double beta = 0.1;
double abs_double(double val) { return  (val < 0) ? val * (-1) : val; }
uint32_t abs_int(int val) { return  (val < 0) ? val * (-1) : val; }
uint64_t abs_int64(int64_t val) { return (val < 0) ? val * (-1) : val; }

void mem_hs_sched_ketama(void)
{
  struct ketama_stat *ketama_stat = get_ketama_stat();
  static uint32_t log_time = 1;

  if ( ketama_stat == NULL )
    return;

  /*
    uint32_t ref;
    bool initialized_;
    uint32_t max; // max usage
    uint32_t maxid; // max usage index
    struct sched_context *ketama_servers;
  */


  if ( ketama_stat->continuum_points_counter == 1 ) {
    // no scheduling required
  } else {
    uint32_t max = 0;

    // only hashsize
    for(uint32_t j=0; j < ketama_stat->continuum_points_counter; j++) {
        uint32_t k = (j + 1) % ketama_stat->continuum_points_counter;
        memcached_continuum_item_st *curr = ketama_stat->continuum + j;
        struct sched_context *host = ketama_stat->servers + curr->index;

        host->hashsize += hs_round(ketama_stat->continuum[j].value, ketama_stat->continuum[k].value);

        /*
        host->get += curr->sched.get;
        host->set += curr->sched.set;
        host->sum += curr->sched.sum;

        if ( host->sum > max )
          max = host->sum;
        */
    }

    for (uint32_t i = 0; i < ketama_stat->continuum_count; i++) {
      struct sched_context *host = ketama_stat->servers + i;
      host->sum = host->get + host->set;
      if ( host->sum > max )
        max = host->sum;
    }

    // simple stats to see
    fprintf(stderr, "%u ", log_time++);
    for (uint32_t i = 0; i < ketama_stat->continuum_count; i++) {
      struct sched_context *host = ketama_stat->servers + i;

      if ( host->get != 0 && (int32_t)host->set <= (int32_t)(host->get - host->set) ) {
        host->hitrate = get_hitrate(host->get, host->set);
        host->nhitrate = host->hitrate;
      }

      host->usagerate = (double)((double)host->sum/(double)max);

      /*
      fprintf(stderr, "[%d] hit(%f), usagerate(%f), cost(%f), set/get(%u/%u) \n", 
                      i, host->hitrate, host->usagerate, scost(host), host->set, host->get);
      */

      fprintf(stderr, "%f %f %f %f %lu %u %u ", 
              host->hitrate, host->nhitrate, host->usagerate, scost(host), host->hashsize, host->set, host->get);

    // reset all
#if 1
      host->get = 0;
      host->set = 0;
      host->sum = 0;
      host->hitrate = 0;
      host->usagerate = 0;
      host->hashsize = 0;
#endif
    }
    ketama_stat->max = 0;
    ketama_stat->maxid = 0;

    fprintf(stderr, "\n");
  }
}

void mem_hs_sched_dynamic(void)
{
  struct sched_dynamic *dynamic = get_dynamic();
  double max_cost=0;
  double max_cost_i=0, max_cost_j=0, max_cost_k=0;
  uint32_t max_i=0, max_j=0, max_k=0;
  bool schedulable = false;
  bool readiness = true;

  double max_hr = 0;

  static uint32_t log_time = 1;

  //fprintf(stderr, "scheduler called.... \n");

  if ( dynamic == NULL )
    return;
  
  //fprintf(stderr, "check alpha = %f, beta = %f\n", dynamic->alpha, dynamic->beta);

  //fprintf(stderr, "dynamic(%p) scheduler called.... %u : %u\n", dynamic, dynamic->continuum_count, dynamic->continuum_points_counter);

#define cost(_i) (scost((&(dynamic->continuum[_i].sched))))
#define hr(_i) (dynamic->continuum[_i].sched.hitrate)
#define ur(_i) (dynamic->continuum[_i].sched.usagerate)

  if ( dynamic->continuum_points_counter == 1 ) {
    // no scheduling required
  } else {
    uint32_t max = 0;

    // server sched information update
    for ( uint32_t h = 0; h < dynamic->continuum_count; h++ ) {
      struct sched_context *host = dynamic->servers + h;
      host->get = 0; host->set = 0; host->sum = 0; 
      //host->nhitrate = 0; host->hitrate = 0; 
      host->usagerate = 0; host->hashsize= 0;
    }

    // server statistics... hashsize
    for(uint32_t j=0; j < dynamic->continuum_points_counter; j++) {
        uint32_t k = (j + 1) % dynamic->continuum_points_counter;
        memcached_continuum_item_st *curr = dynamic->continuum + j;
        struct sched_context *host = dynamic->servers + curr->index;

        host->hashsize += hs_round(dynamic->continuum[j].value, dynamic->continuum[k].value);

        host->get += curr->sched.get;
        host->set += curr->sched.set;
        host->sum += curr->sched.sum;

        if ( host->sum > max )
          max = host->sum;
    }

    // find max hitrate in order to normalize
    for ( uint32_t h = 0; h < dynamic->continuum_count; h++ ) {
      struct sched_context *host = dynamic->servers + h;

      if ( host->get != 0 && (int32_t)host->set <= (int32_t)(host->get - host->set) )
        host->hitrate = get_hitrate(host->get, host->set);

      if ( host->hitrate > max_hr )
        max_hr = host->hitrate;
    }

    fprintf(stderr, "%u ---s--- ", log_time++);
    for ( uint32_t h = 0; h < dynamic->continuum_count; h++ ) {
      struct sched_context *host = dynamic->servers + h;

      if ( max_hr == 0 )
        max_hr = 1;

      host->nhitrate = host->hitrate / max_hr; // normalization to make it same as usagerate
      host->usagerate = (double)((double)host->sum/(double)max);

      // for stat
      host->psum = host->sum;

      //fprintf(stderr, "s[%u] hit(%f), usage(%f), cost(%f), set/get(%u/%u) \n", h, host->hitrate, host->usagerate, scost(host), host->set, host->get);

      // hitrate, usagerate, cost, hash_size, set, get
      fprintf(stderr, "%f %f %f %f %u %u %u ", 
              host->hitrate, host->nhitrate, host->usagerate, scost(host), host->hashsize, host->set, host->get);

      if ( host->nhitrate == 0 || host->usagerate == 0 )
        readiness = false;
    }
    fprintf(stderr, "\n");

    // XXX DEBUG
    fprintf(stderr, "%u ---c--- ", log_time);
    for(uint32_t j=0; j < dynamic->continuum_points_counter; j++) {
      uint32_t i = (j - 1 + dynamic->continuum_points_counter) % dynamic->continuum_points_counter;
      memcached_continuum_item_st *one = dynamic->continuum + j;
      memcached_continuum_item_st *pre = dynamic->continuum + i;

      fprintf(stderr, "%f %f %f %u ", 
              one->sched.hitrate, one->sched.usagerate, cost(i), hs_round(pre->value, one->value));
    }
    fprintf(stderr, "\n");

    // if hitrate and usage are not ready, wait until they are ready.... NOT NOW.. 
    // it turns out it never comes back.. so it is better to make it changes
/*
    if ( ! readiness )
      return;
*/

    // find min-max
    uint32_t srv_min_id = 0;
    uint32_t srv_max_id = 0;
    double maxcost = 0;
    double mincost = 1000;
    bool srv_succ = true;

    // server level: min-max servers
    for(uint32_t j=0; j < dynamic->continuum_count; j++) {
        struct sched_context *host = dynamic->servers + j;
        double cost = scost(host);

        if ( cost > maxcost ) {
          maxcost = cost;
          srv_max_id = j;
        }

        if ( cost < mincost ) {
          mincost = cost;
          srv_min_id = j;
        }
    }

    //fprintf(stderr, "[min, max] : [%u, %u]\n", srv_min_id, srv_max_id);

    if ( srv_max_id == srv_min_id ) 
      srv_succ = false;

    // virtual node level: find a location
    bool cw = true;
    bool max_cw = true;

    if ( srv_succ ) {
      // find maxdiff from selected servers
      for(uint32_t j=0; j < dynamic->continuum_points_counter; j++) {
        uint32_t i = (j - 1 + dynamic->continuum_points_counter) % dynamic->continuum_points_counter;
        uint32_t k = (j + 1) % dynamic->continuum_points_counter;
        double cost;
        
        if ( (srv_max_id == dynamic->continuum[j].index && srv_min_id == dynamic->continuum[k].index) || 
              (srv_min_id == dynamic->continuum[j].index && srv_max_id == dynamic->continuum[k].index) ) {
          cost = cost(j) - cost(k);
          //cw = (cost < 0); // true: clockwise, false: counter-

          cw = (srv_min_id == dynamic->continuum[j].index);
          cost = abs_double(cost);

          if ( cost > max_cost ) {
            max_cost = cost;
            max_cost_i = cost(i);
            max_cost_j = cost(j);
            max_cost_k = cost(k);
            max_i = i;
            max_j = j;
            max_k = k;
            max_cw = cw;
            schedulable = true;
          }
        }
      }
    } else {
      // just find maxdiff cost
      for(uint32_t j=0; j < dynamic->continuum_points_counter; j++) {
          uint32_t i = (j - 1 + dynamic->continuum_points_counter) % dynamic->continuum_points_counter;
          uint32_t k = (j + 1) % dynamic->continuum_points_counter;
          double cost = cost(j) - cost(k);
          cw = (cost < 0); // true: clockwise, false: counter-
          cost = abs_double(cost);

          if ( cost > max_cost ) {
            max_cost = cost;
            max_cost_i = cost(i);
            max_cost_j = cost(j);
            max_cost_k = cost(k);
            max_i = i;
            max_j = j;
            max_k = k;
            max_cw = cw;
            schedulable = true;
          }
      }
    }

    // actual move: max_cost_i,j,k, max_i,j,k, max_cw
    if ( schedulable ) {
      double rate; uint32_t amount;
      if ( max_cw ) {
        rate = (max_cost_k > max_cost_j) ? max_cost_j/max_cost_k : max_cost_k/max_cost_j;
        amount = dynamic->beta * (1.0 - rate) * 
                abs_int(dynamic->continuum[max_j].value - dynamic->continuum[max_k].value);

        if ( abs_int(dynamic->continuum[max_k].value - dynamic->continuum[max_j].value) > amount ) {
              dynamic->continuum[max_j].value += amount;

              fprintf(stderr, "[s:%u -> %u, c:%u -> %u] moving %u of %u \n", 
                      dynamic->continuum[max_j].index,dynamic->continuum[max_k].index, 
                      max_j, max_k, amount, 
                      abs_int(dynamic->continuum[max_k].value - dynamic->continuum[max_j].value));
        }
      } else {
        rate = (max_cost_j > max_cost_i) ? max_cost_i/max_cost_j : max_cost_j/max_cost_i;
        amount = dynamic->beta * (1.0 - rate) * 
                abs_int(dynamic->continuum[max_i].value - dynamic->continuum[max_j].value);

        if ( abs_int(dynamic->continuum[max_j].value - dynamic->continuum[max_i].value) > amount ) {
              dynamic->continuum[max_j].value -= amount;

              fprintf(stderr, "[s:%u -> %u, c:%u -> %u] moving %u of %u\n", 
                      dynamic->continuum[max_j].index,dynamic->continuum[max_i].index, 
                      max_j, max_i, amount, 
                      abs_int(dynamic->continuum[max_j].value - dynamic->continuum[max_i].value));
        }
      }
    }

    // reset all for safety falling in infinite 0 hitrate
    if ( schedulable ) {
      dynamic->max = 0;
      dynamic->maxid = 0;
      for ( uint32_t h = 0; h < dynamic->continuum_points_counter; h++ ) {
        memcached_continuum_item_st *curr = dynamic->continuum + h;
        struct sched_context *host = dynamic->servers + curr->index;

        curr->sched.get = 0;
        curr->sched.set = 0;
        curr->sched.sum = 0;
        //curr->sched.hitrate = 0; 
        curr->sched.usagerate = 0;

        host->get = 0;
        host->set = 0;
        //host->sum = 0; // this number is needed by controller
        // can be 0.. so just inherit from previous one
        // or I can skip this time to schedule wait until it is ready
        //host->hitrate = 0;
        host->usagerate = 0;
        host->hashsize = 0;
      }
    }
  }

  //fprintf(stderr, "max_cost(%f), s(%u), t(%u), dynamic(%p)\n", max_cost, max_cost_s, max_cost_t, dynamic);
}

// jinho added - should be called when hash updated from scheduler...???
#define trace() printf("%s(%d) \n", __FILE__, __LINE__);
//#define trace()
static memcached_return_t update_dynamic(memcached_st *ptr)
{
  uint32_t continuum_index= 0;
  uint32_t pointer_counter= 0;
  uint32_t pointer_per_server= MEMCACHED_POINTS_PER_SERVER;
  uint32_t pointer_per_hash= 1;
  uint32_t live_servers= 0;
  struct timeval now;
  uint32_t i,j;

  if (gettimeofday(&now, NULL))
  {
    return memcached_set_errno(*ptr, errno, MEMCACHED_AT);
  }

  // each server information
  org::libmemcached::Instance* list= memcached_instance_list(ptr);

  // check scheduler
  //if ( ptr->dynamic->initialized_ )
  //  return MEMCACHED_SUCCESS;

  /* count live servers (those without a retry delay set) */
  bool is_auto_ejecting= _is_auto_eject_host(ptr);
  if (is_auto_ejecting)
  {
    live_servers= 0;
    ptr->dynamic->next_distribution_rebuild= 0;
    for (uint32_t host_index= 0; host_index < memcached_server_count(ptr); ++host_index)
    {
      if (list[host_index].next_retry <= now.tv_sec)
      {
        live_servers++;
      }
      else
      {
        if (ptr->dynamic->next_distribution_rebuild == 0 or list[host_index].next_retry < ptr->dynamic->next_distribution_rebuild)
        {
          ptr->dynamic->next_distribution_rebuild= list[host_index].next_retry;
        }
      }
    }
  }
  else
  {
    live_servers= memcached_server_count(ptr);
  }
//printf("current live_servers = %d, ptr->dynamic->continuum_count = %d \n", live_servers, ptr->dynamic->continuum_count);

  // just return when the same number.. # of servers was not changed
  if ( live_servers == 0 || ptr->dynamic->continuum_count == live_servers ) 
    return MEMCACHED_SUCCESS;

  if ( ptr->dynamic->continuum_points_counter == 0 ) // initial assignment
  {
    //uint32_t points_per_server = (uint32_t) (memcached_is_dynamic(ptr) ? MEMCACHED_POINTS_PER_SERVER_KETAMA : MEMCACHED_POINTS_PER_SERVER);
    uint32_t points_per_server = (live_servers == 1 ) ? 1 : live_servers - 1; // one set
    uint32_t set_size = live_servers * points_per_server;
    uint32_t total_points = set_size * MEMCACHED_DYNAMIC_SET_REPETITION; // leave this way for future addition/removal
    //MEMCACHED_DYNAMIC_SET_REPETITION

//printf("initial assignment... entered..\n");

  //printf("live_servers(%d), ptr->dynamic.continuum_count(%d) \n", live_servers, ptr->dynamic.continuum_count);

    // always the case
    if (live_servers > ptr->dynamic->continuum_count)
    {
      memcached_continuum_item_st *new_ptr;

  //printf("live_servers(%d) + MEMCACHED_CONTINUUM_ADDITION(%d), points_per_server(%d) \n", live_servers, MEMCACHED_CONTINUUM_ADDITION, points_per_server);

      new_ptr= libmemcached_xrealloc(ptr, ptr->dynamic->continuum, total_points, memcached_continuum_item_st);

      if (new_ptr == 0)
        return MEMCACHED_MEMORY_ALLOCATION_FAILURE;

      ptr->dynamic->continuum= new_ptr;
      ptr->dynamic->continuum_count= live_servers;
    }

    // SERVERS sched_context
    if ( live_servers > 0 ) {
      struct sched_context *new_srv_ptr;

      new_srv_ptr = (struct sched_context*)libmemcached_malloc(NULL, sizeof(struct sched_context) * live_servers);

      if ( new_srv_ptr == 0 )
        return MEMCACHED_MEMORY_ALLOCATION_FAILURE;

      ptr->dynamic->servers = new_srv_ptr;

      for(uint32_t host_index = 0; host_index < live_servers; ++host_index) {
        struct sched_context *host_pointer = new_srv_ptr + host_index;
        host_pointer->get = 0;
        host_pointer->set = 0;
        host_pointer->sum = 0;
        host_pointer->hitrate = 0;
        host_pointer->usagerate = 0;
      }
    }

    // XXX WEIGHT - I do not consier weights for now...
    uint64_t total_weight= 0;
    if (memcached_is_weighted_dynamic(ptr))
    {
      for (uint32_t host_index = 0; host_index < memcached_server_count(ptr); ++host_index)
      {
        if (is_auto_ejecting == false or list[host_index].next_retry <= now.tv_sec)
        {
          total_weight += list[host_index].weight;
        }
      }
    }
//printf("----- set_size = %d\n", set_size);

    // INCIPIENT ALGORITHM
    uint32_t *set = (uint32_t*)libmemcached_malloc(NULL, sizeof(uint32_t)*set_size);
    if ( set == NULL ) { return MEMCACHED_MEMORY_ALLOCATION_FAILURE; }

    uint32_t **bak = (uint32_t**)libmemcached_malloc(NULL, sizeof(uint32_t*)*live_servers);
    if ( bak == NULL ) { return MEMCACHED_MEMORY_ALLOCATION_FAILURE; }

    for(i=0; i < live_servers; i++) {
      bak[i] = (uint32_t*)libmemcached_malloc(NULL, sizeof(uint32_t)*live_servers);
    }

    // XXX for now, use this due to moxi: free(): invalid next size (fast)
    //uint32_t set[set_size];
    //uint32_t bak[live_servers][live_servers];

    for(i=0; i < set_size; i++) {
      set[i] = 0;
    }

    for(i=0; i < live_servers; i++) {
      for(j=0; j < live_servers; j++) {
        if ( i == j ) {
          bak[i][j] = 1;
        } else {
          bak[i][j] = 0;
        }
      }
    }

    // host 0 - (n-1)
    uint32_t cur_nd = 0;
    uint32_t set_id = 1; // first is alwasys 0
    uint32_t cur_row = 0;
    uint32_t cur_frow = 0;

    for(i=0; i < set_size-1; i++) {
      for(j=0; j < live_servers; j++) {
        cur_row = (cur_frow + j) % live_servers;
        if ( bak[cur_nd][cur_row] == 0 ) { // always at least one
          bak[cur_nd][cur_row] = 1; 
          set[set_id++] = cur_row;
          cur_nd = cur_row;
          cur_frow = (cur_row + 1) % live_servers; 
          break;
        }    
      }    
    }  

//printf("------- set_id = %d\n", set_id);

    // DEBUG
/*
    for(i=0; i < set_size; i++) {
      printf("%u -> ", set[i] + 1);
    }
    printf("\n");
*/

    // incipient assignment
    uint32_t unit = 0xFFFFFFFF / total_points;
    //printf("total_points = %u, unit = %u\n", total_points, unit);
    ptr->dynamic->max = 0;
    ptr->dynamic->maxid = 0;
    for (uint32_t index= 0; index < total_points; ++index)
    {
        uint32_t value = unit * (index + 1);

        ptr->dynamic->continuum[continuum_index].sched.get = 0;
        ptr->dynamic->continuum[continuum_index].sched.set = 0;
        ptr->dynamic->continuum[continuum_index].sched.sum = 0;

        ptr->dynamic->continuum[continuum_index].sched.hitrate = 0;
        ptr->dynamic->continuum[continuum_index].sched.usagerate = 0;

        ptr->dynamic->continuum[continuum_index].index= set[(index % set_size)];
        ptr->dynamic->continuum[continuum_index++].value= value;
    }

    ptr->dynamic->continuum_points_counter= total_points;

    libmemcached_free(NULL, set);
    for(i=0; i < live_servers; i++) {
      libmemcached_free(NULL, bak[i]);
    }
    libmemcached_free(NULL, bak);
  } else { // addition / removal

    double max_cost = 0;
    uint32_t max_id = 0; // default

    // live_servers = new index

    if ( live_servers > ptr->dynamic->continuum_count ) { // addition
      // continumm assign...,but do not change the mapping..
      printf("addition assignment... entered..\n");

      // find the most cost 
      // FIXME XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX FIXME
      // it needs to be stable value instead of instant cost... maybe accumulated cost???
      // scheduler can determine one for this...
      for(uint32_t host_index = 0; host_index < ptr->dynamic->continuum_count; ++host_index) {
        struct sched_context *phost = ptr->dynamic->servers + host_index;
        //double c = scost(phost);
        double c = phost->usagerate;

        if ( c > max_cost ) {
          max_cost = c;
          max_id = host_index;
        }
      }

      // find the number of this server
      uint32_t srv_num = 0;
      for (uint32_t index= 0; index < ptr->dynamic->continuum_points_counter; ++index)
      {
        if ( ptr->dynamic->continuum[index].index == max_id ) {
          srv_num ++;
        }
      }

//printf("max_cost = %f, max_id = %u, srv_num = %d\n", max_cost, max_id, srv_num);

      uint32_t old_total_points = ptr->dynamic->continuum_points_counter;
      uint32_t new_total_points = ptr->dynamic->continuum_points_counter + srv_num;

      memcached_continuum_item_st *new_ptr;
      memcached_continuum_item_st *old_ptr = ptr->dynamic->continuum;

      new_ptr= (memcached_continuum_item_st*)libmemcached_malloc(ptr, new_total_points * sizeof(memcached_continuum_item_st));

      if (new_ptr == 0)
        return MEMCACHED_MEMORY_ALLOCATION_FAILURE;

      int ccnt = 0;
      for (uint32_t j = 0; j < old_total_points; ++j)
      {
        uint32_t i = (j - 1 + ptr->dynamic->continuum_points_counter) % ptr->dynamic->continuum_points_counter;
        memcached_continuum_item_st *temp;

        if ( old_ptr[j].index == max_id ) {
          uint32_t hs = hs_round((ptr->dynamic->continuum + i)->value, (ptr->dynamic->continuum + j)->value)/2;
          uint32_t new_value = hs_round(hs, ptr->dynamic->continuum[j].value);

//printf("%u - (%u - %u)/2 = %u \n", ptr->dynamic->continuum[j].value, ptr->dynamic->continuum[j].value, ptr->dynamic->continuum[i].value, new_value);

          temp = new_ptr + ccnt;

          temp->sched.get = 0;
          temp->sched.set = 0;
          temp->sched.sum = 0;

          temp->sched.hitrate = 0;
          temp->sched.usagerate = 0;

          temp->index = live_servers - 1; // new server at the end..
          temp->value = new_value;

          ccnt ++;
        }

        // continuum info + sched info
        temp = new_ptr + ccnt;
        memcpy(temp, old_ptr + j, sizeof(memcached_continuum_item_st));
        ccnt ++;
      }

      ptr->dynamic->continuum_points_counter= new_total_points;
      ptr->dynamic->continuum= new_ptr;
      ptr->dynamic->continuum_count= live_servers;

      libmemcached_free(ptr, old_ptr);

    } else if ( live_servers < ptr->dynamic->continuum_count ) { // removal

//printf("deletion assignment... entered..\n");

//printf("XXXXXXXXX removing_idx in update = %d \n", ptr->dynamic->removing_idx);

      int removing_points = 0;
      for (uint32_t index= 0; index < ptr->dynamic->continuum_points_counter; ++index)
      {
        if ( ptr->dynamic->continuum[index].index == ptr->dynamic->removing_idx ) {
          removing_points ++;
        }
      }

      uint32_t old_total_points = ptr->dynamic->continuum_points_counter;
      uint32_t new_total_points = ptr->dynamic->continuum_points_counter - removing_points;

      memcached_continuum_item_st *new_ptr;
      memcached_continuum_item_st *old_ptr = ptr->dynamic->continuum;

      // old_total_points >= removing_points
      WATCHPOINT_ASSERT(old_total_points >= removing_points);

      new_ptr= (memcached_continuum_item_st*)libmemcached_malloc(ptr, new_total_points * sizeof(memcached_continuum_item_st));

      if (new_ptr == 0)
        return MEMCACHED_MEMORY_ALLOCATION_FAILURE;

      int ccnt = 0;
      for (uint32_t index= 0; index < old_total_points; ++index)
      {
        if ( old_ptr[index].index != ptr->dynamic->removing_idx ) {
          memcached_continuum_item_st *temp = new_ptr + ccnt;

          // continuum info + sched info
          memcpy(temp, old_ptr + index, sizeof(memcached_continuum_item_st));
          ccnt ++;

          // only when the index is bigger than the one removed
          if ( temp->index > ptr->dynamic->removing_idx )
            temp->index --;
        }
      }

      ptr->dynamic->continuum_points_counter= new_total_points;
      ptr->dynamic->continuum= new_ptr;
      ptr->dynamic->continuum_count= live_servers;

      libmemcached_free(ptr, old_ptr);
    } 

    ptr->dynamic->continuum_count= live_servers;

    // SERVERS sched_context
    if ( live_servers > 0 ) {
      struct sched_context *new_srv_ptr;
      struct sched_context *old_srv_ptr = ptr->dynamic->servers;

      new_srv_ptr = (struct sched_context*)libmemcached_malloc(NULL, sizeof(struct sched_context) * live_servers);

      if ( new_srv_ptr == 0 )
        return MEMCACHED_MEMORY_ALLOCATION_FAILURE;

      ptr->dynamic->servers = new_srv_ptr;

      for(uint32_t host_index = 0; host_index < live_servers; ++host_index) {
        struct sched_context *host_pointer = new_srv_ptr + host_index;
        host_pointer->get = 0;
        host_pointer->set = 0;
        host_pointer->sum = 0;
        host_pointer->hitrate = 0;
        host_pointer->usagerate = 0;
      }

      libmemcached_free(NULL, old_srv_ptr);
    }

    // in case we have rounded hash space assignment
    qsort(ptr->dynamic->continuum, ptr->dynamic->continuum_points_counter, sizeof(memcached_continuum_item_st), continuum_item_cmp);
  }

  // XXX DEBUG
  for(i=0; i < ptr->dynamic->continuum_points_counter; i++) {
    printf("%u (%u) -> ", ptr->dynamic->continuum[i].index, ptr->dynamic->continuum[i].value);
  }
  printf("\n");

  WATCHPOINT_ASSERT(ptr);
  WATCHPOINT_ASSERT(ptr->dynamic->continuum);

  ptr->dynamic->initialized_ = true;

  return MEMCACHED_SUCCESS;
}

static memcached_return_t server_add(memcached_st *ptr, 
                                     const memcached_string_t& hostname,
                                     in_port_t port,
                                     uint32_t weight,
                                     memcached_connection_t type)
{
  assert_msg(ptr, "Programmer mistake, somehow server_add() was passed a NULL memcached_st");

  org::libmemcached::Instance* new_host_list= libmemcached_xrealloc(ptr, memcached_instance_list(ptr), (ptr->number_of_hosts + 1), org::libmemcached::Instance);

  if (new_host_list == NULL)
  {
    return memcached_set_error(*ptr, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT);
  }

  memcached_instance_set(ptr, new_host_list);

  /* TODO: Check return type */
  org::libmemcached::Instance* instance= memcached_instance_fetch(ptr, memcached_server_count(ptr));

  if (__instance_create_with(ptr, instance, hostname, port, weight, type) == NULL)
  {
    return memcached_set_error(*ptr, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT);
  }

  if (weight > 1)
  {
    if (memcached_is_consistent_distribution(ptr))
    {
      memcached_set_weighted_ketama(ptr, true);
    }
  }

  ptr->number_of_hosts++;

  return run_distribution(ptr);
}

memcached_return_t get_sched_stat(char *buf, int buf_len)
{
  struct sched_dynamic *dynamic = get_dynamic();
  int buf_cnt = 0;
  char temp[64];

//fprintf(stderr, "called...\n");

  if ( dynamic == NULL )
    return MEMCACHED_SUCCESS;

  for ( uint32_t h = 0; h < dynamic->continuum_count; h++ ) {
      struct sched_context *host = dynamic->servers + h;

//fprintf(stderr, "------ %u\n", host->psum);

      buf_cnt = sprintf(buf, "%s,%u", buf, host->psum); 

      if ( (buf_cnt + sprintf(temp, "%u", host->psum)) >= buf_len )
        break;
  }
//fprintf(stderr, "done...%s \n", buf);

  return MEMCACHED_SUCCESS;
}

//#define trace() printf("%s(%d)\n", __FILE__, __LINE__);
memcached_return_t memcached_server_push(memcached_st *ptr, const memcached_server_list_st list)
{
  if (list == NULL)
  {
    return MEMCACHED_SUCCESS;
  }

  /*
    for(int i = 0; i < list->number_of_hosts; i++) {
        memcached_server_st *temp = list + i;
        printf("--------- memcached_server_push host = %s, %d\n", temp->hostname, temp->port);
    }
  */

  uint32_t count= memcached_server_list_count(list);

  org::libmemcached::Instance* new_host_list= libmemcached_xrealloc(ptr, memcached_instance_list(ptr), (count + memcached_server_count(ptr)), org::libmemcached::Instance);

  if (new_host_list == NULL)
  {
    return MEMCACHED_MEMORY_ALLOCATION_FAILURE;
  }

  memcached_instance_set(ptr, new_host_list);

  for (uint32_t x= 0; x < count; x++)
  {
    WATCHPOINT_ASSERT(list[x].hostname[0] != 0);

    // We have extended the array, and now we will find it, and use it.
    org::libmemcached::Instance* instance= memcached_instance_fetch(ptr, memcached_server_count(ptr));
    WATCHPOINT_ASSERT(instance);

    memcached_string_t hostname= { memcached_string_make_from_cstr(list[x].hostname) };
    if (__instance_create_with(ptr, instance, 
                               hostname,
                               list[x].port, list[x].weight, list[x].type) == NULL)
    {
      return memcached_set_error(*ptr, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT);
    }

    if (list[x].weight > 1)
    {
      memcached_set_weighted_ketama(ptr, true);
    }

    ptr->number_of_hosts++;
  }

  return run_distribution(ptr);
}

// jinho added
memcached_return_t memcached_server_remove(memcached_st *ptr, const memcached_server_list_st list)
{
  // XXX deal only one for now...

  org::libmemcached::Instance* dst_inst = NULL;
  org::libmemcached::Instance* old = NULL;

  if (list == NULL)
  {
    return MEMCACHED_SUCCESS;
  }

  for(int i = 0; i < memcached_server_count(ptr); i++) {
      org::libmemcached::Instance* temp = ptr->servers + i;
// printf("--------- memcached_server_remove host = %s, %d\n", temp->hostname, temp->port());

      if ( strcmp(temp->hostname, list->hostname) == 0 ) {
        dst_inst = temp;
        break;
      }
  }
  
  if ( dst_inst == NULL )
    return MEMCACHED_NOTFOUND;

//printf("found server hostname = %s \n", dst_inst->hostname);

  uint32_t count= memcached_server_list_count(list);

  if ( (memcached_server_count(ptr) - count) <= 0 )
    return MEMCACHED_SOME_ERRORS;

  // XXX this needs to be changed...
  org::libmemcached::Instance* new_host_list= (org::libmemcached::Instance*)libmemcached_malloc(ptr, (memcached_server_count(ptr) - count) * sizeof(org::libmemcached::Instance));
  if (new_host_list == NULL)
  {
    return MEMCACHED_MEMORY_ALLOCATION_FAILURE;
  }

  old = ptr->servers;
  memcached_instance_set(ptr, new_host_list);

  // copy
  int scnt = 0;
  int removing_idx = -1;
  for(int i = 0; i < ptr->number_of_hosts; i++) {
    org::libmemcached::Instance *inst = old + i;

    // XXX XXX XXX XXX XXX for now, just see one host
//printf("------ inst->hostname (%s), list->hostname(%s) \n", inst->hostname, list->hostname);
    if ( strcmp(inst->hostname, list->hostname) != 0 ) {
      memcpy(new_host_list + scnt, inst, sizeof(org::libmemcached::Instance));
      scnt ++;
    } else {
      removing_idx = i;
    }
  }
  ptr->number_of_hosts -= count;
  ptr->dynamic->removing_idx = removing_idx;

  libmemcached_free(ptr, old);

//printf("server removed... removing_idx (%d)....scnt (%d) == ptr->number_of_hosts (%d) \n", removing_idx, scnt, ptr->number_of_hosts);

#if 0
  for (uint32_t x= 0; x < count; x++)
  {
    WATCHPOINT_ASSERT(list[x].hostname[0] != 0);

    // We have extended the array, and now we will find it, and use it.
    org::libmemcached::Instance* instance= memcached_instance_fetch(ptr, memcached_server_count(ptr));
    WATCHPOINT_ASSERT(instance);

    memcached_string_t hostname= { memcached_string_make_from_cstr(list[x].hostname) };
    if (__instance_create_with(ptr, instance, hostname, list[x].port, list[x].weight, list[x].type) == NULL)
    {
      return memcached_set_error(*ptr, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT);
    }

    if (list[x].weight > 1)
    {
      memcached_set_weighted_ketama(ptr, true);
    }

    ptr->number_of_hosts++;
  }
#endif

  return run_distribution(ptr); // reschedule hash space
}

memcached_return_t memcached_instance_push(memcached_st *ptr, const struct org::libmemcached::Instance* list, uint32_t number_of_hosts)
{
  if (list == NULL)
  {
    return MEMCACHED_SUCCESS;
  }

  org::libmemcached::Instance* new_host_list= libmemcached_xrealloc(ptr, memcached_instance_list(ptr), (number_of_hosts +memcached_server_count(ptr)), org::libmemcached::Instance);

  if (new_host_list == NULL)
  {
    return MEMCACHED_MEMORY_ALLOCATION_FAILURE;
  }

  memcached_instance_set(ptr, new_host_list);

  for (uint32_t x= 0; x < number_of_hosts; x++)
  {

    WATCHPOINT_ASSERT(list[x].hostname[0] != 0);

    // We have extended the array, and now we will find it, and use it.
    org::libmemcached::Instance* instance= memcached_instance_fetch(ptr, memcached_server_count(ptr));
    WATCHPOINT_ASSERT(instance);

    memcached_string_t hostname= { memcached_string_make_from_cstr(list[x].hostname) };
    if (__instance_create_with(ptr, instance, 
                               hostname,
                               list[x].port(), list[x].weight, list[x].type) == NULL)
    {
      return memcached_set_error(*ptr, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT);
    }

    if (list[x].weight > 1)
    {
      memcached_set_weighted_ketama(ptr, true);
    }

    ptr->number_of_hosts++;
  }

  return run_distribution(ptr);
}

memcached_return_t memcached_server_add_unix_socket(memcached_st *ptr,
                                                    const char *filename)
{
  return memcached_server_add_unix_socket_with_weight(ptr, filename, 0);
}

memcached_return_t memcached_server_add_unix_socket_with_weight(memcached_st *ptr,
                                                                const char *filename,
                                                                uint32_t weight)
{
  if (ptr == NULL)
  {
    return MEMCACHED_FAILURE;
  }

  memcached_string_t _filename= { memcached_string_make_from_cstr(filename) };
  if (memcached_is_valid_servername(_filename) == false)
  {
    memcached_set_error(*ptr, MEMCACHED_INVALID_ARGUMENTS, MEMCACHED_AT, memcached_literal_param("Invalid filename for socket provided"));
  }

  return server_add(ptr, _filename, 0, weight, MEMCACHED_CONNECTION_UNIX_SOCKET);
}

memcached_return_t memcached_server_add_udp(memcached_st *ptr,
                                            const char *hostname,
                                            in_port_t port)
{
  return memcached_server_add_udp_with_weight(ptr, hostname, port, 0);
}

memcached_return_t memcached_server_add_udp_with_weight(memcached_st *ptr,
                                                        const char *,
                                                        in_port_t,
                                                        uint32_t)
{
  if (ptr == NULL)
  {
    return MEMCACHED_INVALID_ARGUMENTS;
  }

  return memcached_set_error(*ptr, MEMCACHED_DEPRECATED, MEMCACHED_AT);
}

memcached_return_t memcached_server_add(memcached_st *ptr,
                                        const char *hostname,
                                        in_port_t port)
{
  return memcached_server_add_with_weight(ptr, hostname, port, 0);
}

memcached_return_t memcached_server_add_with_weight(memcached_st *ptr,
                                                    const char *hostname,
                                                    in_port_t port,
                                                    uint32_t weight)
{
  if (ptr == NULL)
  {
    return MEMCACHED_INVALID_ARGUMENTS;
  }

  if (port == 0)
  {
    port= MEMCACHED_DEFAULT_PORT;
  }

  size_t hostname_length= hostname ? strlen(hostname) : 0;
  if (hostname_length == 0)
  {
    hostname= "localhost";
    hostname_length= memcached_literal_param_size("localhost");
  }

  memcached_string_t _hostname= { hostname, hostname_length };

  if (memcached_is_valid_servername(_hostname) == false)
  {
    return memcached_set_error(*ptr, MEMCACHED_INVALID_ARGUMENTS, MEMCACHED_AT, memcached_literal_param("Invalid hostname provided"));
  }

  return server_add(ptr, _hostname, port, weight, _hostname.c_str[0] == '/' ? MEMCACHED_CONNECTION_UNIX_SOCKET  : MEMCACHED_CONNECTION_TCP);
}

memcached_return_t memcached_server_add_parsed(memcached_st *ptr,
                                               const char *hostname,
                                               size_t hostname_length,
                                               in_port_t port,
                                               uint32_t weight)
{
  char buffer[NI_MAXHOST];

  memcpy(buffer, hostname, hostname_length);
  buffer[hostname_length]= 0;

  memcached_string_t _hostname= { buffer, hostname_length };

  return server_add(ptr, _hostname,
                    port,
                    weight,
                    MEMCACHED_CONNECTION_TCP);
}
