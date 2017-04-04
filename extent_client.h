// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "extent_server.h"
#include <pthread.h>
#include <map>
#include<sys/time.h>

#define DIRTY true
#define CLEAN false


using namespace std;
struct FileCache
{
	int eid;
	bool dirty;
	bool valid;
	string content;
	extent_protocol::attr attr;

	FileCache(){
		eid = -1;
		dirty = CLEAN;
		valid  = false;
		content = "";
	}
};


class extent_client {
 private:
  	rpcc *cl;
	map<extent_protocol::extentid_t, FileCache> caches;
	//pthread_cond_t mapCond;
	pthread_mutex_t mapMutex;

    void printCache();
    unsigned getCurrentTime();
 public:
  	extent_client(std::string dst);

  	extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid);
  	extent_protocol::status get(extent_protocol::extentid_t eid,
			                        std::string &buf);
  	extent_protocol::status getattr(extent_protocol::extentid_t eid,
				                          extent_protocol::attr &a);
  	extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  	extent_protocol::status remove(extent_protocol::extentid_t eid);
  	extent_protocol::status flush(extent_protocol::extentid_t eid);
  	extent_protocol::status loadInfo(extent_protocol::extentid_t eid);
};

#endif

