#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>


#include <map>
#include <deque>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include <pthread.h>



#define INVALID -1
#define STATUS_FREE 0
#define STATUS_REVOKING 1
#define STATUS_LOCKED 2

using namespace std;

  struct server_lockInfo{
        int status;
        string owner;
        deque<string> waiting;
        bool revoked;

        server_lockInfo(){
            status = STATUS_FREE;
            owner = "";
            revoked = false;
        }
  };

class lock_server_cache {
 private:
  int nacquire;

  map< lock_protocol::lockid_t, server_lockInfo > locks;
  pthread_mutex_t mapLock;
    int announce_to_client(int operation, lock_protocol::lockid_t lid, string id);
 public:
  lock_server_cache();
  
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
