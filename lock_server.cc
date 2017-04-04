// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>


lock_server::~lock_server()
{
	pthread_mutex_destroy(&maplock);
	pthread_cond_destroy(&wait);
}

lock_server::lock_server():
  nacquire (0)
{
    pthread_mutex_init(&maplock, 0);
    pthread_cond_init(&wait, 0);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{

  lock_protocol::status ret = lock_protocol::OK;
  r = 0;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
    lock_protocol::status ret = lock_protocol::OK;
	// Your lab4 code goes here

    pthread_mutex_lock(&maplock);
    
	set<lock_protocol::lockid_t>::iterator it_lock;
	
	while(locks.find(lid) != locks.end() )
	{
		pthread_cond_wait(&wait,&maplock);
	}
		
	
	it_lock = locks.find(lid);
	//Lock does not exist, try create one.
	if(it_lock == locks.end())
	{
		printf("acquire() create %llu\n", lid);	
        locks.insert(lid);
        it_lock = locks.find(lid);
	}


	 printf("===lock_server::acquire() %llu end===\n", lid);
	pthread_mutex_unlock(&maplock);

  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
	pthread_mutex_lock(&maplock);
    printf("===lock_server::release() %llu begin===\n", lid);
    lock_protocol::status ret = lock_protocol::OK;
	// Your lab4 code goes here
	//pthread_mutex_t templock = PTHREAD_MUTEX_INITIALIZER;
	set<lock_protocol::lockid_t>::iterator it;
    
    it = locks.find(lid);
    if(it == locks.end())
    {
        printf("lock_server::release() lock_id %llu not found\n", lid);
        ret = lock_protocol::NOENT;
        pthread_mutex_unlock(&maplock);
        return ret;
    }
    
    locks.erase(lid);
	pthread_cond_broadcast(&wait);

	printf("===lock_server::release() %llu end===\n", lid);
	pthread_mutex_unlock(&maplock);


    return ret;
}


