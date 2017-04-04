// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"
#include <assert.h>


int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst,
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
      srand(time(NULL)^last_port);
      rlock_port = ((rand()%32000) | (0x1 << 10));
      const char *hname;
      // VERIFY(gethostname(hname, 100) == 0);
      hname = "127.0.0.1";
      std::ostringstream host;
      host << hname << ":" << rlock_port;
      id = host.str();
      last_port = rlock_port;
      rpcs *rlsrpc = new rpcs(rlock_port);
      rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
      rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
      pthread_mutex_init(&mapLock,NULL);
}


lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{

    int ret = lock_protocol::OK;
    pthread_mutex_lock(&mapLock);

    //tprintf("acquire id=%s lid=%llu\n", id.c_str(), lid);
    //Check if the lock has been cached.
     map<lock_protocol::lockid_t, client_lockInfo>::iterator it = locks.find(lid);
    //If lock doesn't exist, insert it into the map
    if(it == locks.end())
    {
        client_lockInfo newLock;
        locks.insert( pair<lock_protocol::lockid_t, client_lockInfo>(lid, newLock));
        it = locks.find(lid);
    }
		int r=0;
    while(1)
    {
        switch(it->second.status)
        {
        case CLIENT_NONE:     //Lock not in the local cache, get it from the server
            //while(!(it->second.isRetry) && it->second.status == CLIENT_ACQUIRING)
				//tprintf("acquire id=%s lid=%llu CLIENT_NONE\n", id.c_str(), lid);
                it->second.status = CLIENT_ACQUIRING;

                while(!it->second.isRetry){
                    pthread_mutex_unlock(&mapLock);
                    //usleep(10000);
                    ret = cl->call(lock_protocol::acquire, lid, id, r);
                    pthread_mutex_lock(&mapLock);
                    //ret = send_msg(lock_protocol::acquire, lid, id);


                    //tprintf("%s requiring %llu\n", id.c_str(), lid);


                    if(it->second.status != CLIENT_ACQUIRING)
                    {
                    	if(lu != 0)
	                        lu->doacquire(lid);
                        goto end;
                    }
                    if(ret == lock_protocol::OK)
                    {
                        //tprintf("%s requiring %llu OK\n", id.c_str(), lid);
                        //tprintf("%s doacquire %llu start\n", id.c_str(), lid);
                        //tprintf("%s lu is 0x%x OK\n", id.c_str(), lu);
                        if(lu != 0)
		                    lu->doacquire(lid);
						//tprintf("%s doacquire %llu OK\n", id.c_str(), lid);
                        it->second.status = CLIENT_LOCKED;
                        goto end;      //Lock acquired, cache it.
                    }
                    if(ret == lock_protocol::RETRY)
                    {
                        //tprintf("%s requiring %llu RETRY\n", id.c_str(), lid);


                            pthread_cond_wait(&(it->second.retry), &mapLock);
                        //tprintf("%s requiring %llu WOKEN\n", id.c_str(), lid);
                            ret = lock_protocol::OK;
                            if(it->second.status ==  CLIENT_LOCKED || it->second.status == CLIENT_REVOKING)
                            {
                            	if(lu != 0)
                               		 lu->doacquire(lid);
                                it->second.isRetry = false;
                                goto end;
                            }

                        it->second.isRetry = false;
                    }
				}
            goto end;
        case CLIENT_CACHING:
            //Lock already acquired or cached, just lock it.
            //tprintf("acquire id=%s lid=%llu CLIENT_CACHING\n", id.c_str(), lid);
            it->second.status = CLIENT_LOCKED;
            goto end;
        default:
              //tprintf("acquire id=%s lid=%llu default, status=%d\n", id.c_str(), lid,it->second.status);
            //while((it->second.status == CLIENT_REVOKING) ||
            //    (it->second.status == CLIENT_LOCKED)  ||
            //    (it->second.status == CLIENT_ACQUIRING))
           // {
                pthread_cond_wait(&(it->second.cond), &mapLock);
           //     break;
           // }
        }
    }

end:

	it->second.isRetry = false;
    //tprintf("acquire leaving, id=%s, lid=%llu  ret=0x%x, status=%d\n", id.c_str(), lid, ret,it->second.status);
    pthread_mutex_unlock(&mapLock);
    return ret;
}




lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{

    pthread_mutex_lock(&mapLock);

    usleep(5000);
    int ret = lock_protocol::OK;
    std::map<lock_protocol::lockid_t, client_lockInfo>::iterator it = locks.find(lid);
    //tprintf("release start: id=%s, lid=%llu status=%d \n",id.c_str(), lid,  it->second.status );
    //If lock doesn't exist, report error
    if(it == locks.end())
    {
        pthread_mutex_unlock(&mapLock);
        return lock_protocol::OK;
    }
    assert(it != locks.end());
    //Sometime before, server called to revoke this lock.

    	if(it->second.isRevoke)
    	{
    		it->second.isRevoke = false;
    		it->second.status = CLIENT_NONE;
        pthread_mutex_unlock(&mapLock);
        int r = 0;
    	ret = cl->call(lock_protocol::release, lid, id, r);
    	if(lu != 0)
	    	lu->dorelease(lid);
    	pthread_mutex_lock(&mapLock);
        //usleep(10000);
        pthread_cond_signal(&(it->second.cond));
        pthread_mutex_unlock(&mapLock);
	    return ret;

    	}

    if(it->second.status == CLIENT_REVOKING)
    {
        //tprintf("release: id=%s, lid=%llu is revoke\n",id.c_str(), lid );
        //ret = send_msg(lock_protocol::release, lid, id);

        it->second.status = CLIENT_NONE;
        pthread_mutex_unlock(&mapLock);
        int r = 0;
        //lu->dorelease(lid);
    	ret = cl->call(lock_protocol::release, lid, id, r);
    	if(lu != 0)
    	 	lu->dorelease(lid);
    	pthread_mutex_lock(&mapLock);
        //usleep(10000);
        pthread_cond_signal(&(it->second.cond));
   //Need to acquire the lock again.

    }
    else if(it->second.status == CLIENT_LOCKED)
    {
        //tprintf("release: id=%s, lid=%llu CLIENT_LOCKED\n",id.c_str(), lid );
        it->second.status = CLIENT_CACHING;     //Indicating that the lock has been used.
        //tprintf("release: id=%s, lid=%llu status=%d\n",id.c_str(), lid ,it->second.status);
        pthread_cond_signal(&it->second.cond);
    }
    else{


        //tprintf("release: id=%s, lid=%llu WRONG STATUS %d\n",id.c_str(), lid, it->second.status );
    }

    //tprintf("release finish : id=%s, lid=%llu status=%d\n",id.c_str(), lid, it->second.status );
    pthread_mutex_unlock(&mapLock);
    return ret;
}






rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid,
                                  int &)
{

    int ret = lock_protocol::OK;
    pthread_mutex_lock(&mapLock);
    std::map<lock_protocol::lockid_t, client_lockInfo>::iterator it = locks.find(lid);
    //If lock doesn't exist, report error
   //tprintf("revoke_handler: id=%s, lid=%llu status=%d\n",id.c_str(), lid ,it->second.status);


    if(it->second.status == CLIENT_LOCKED)
    {
    	//tprintf("revoke: id=%s, lid=%llu  CLIENT_LOCKED\n",id.c_str(), lid );
        it->second.status = CLIENT_REVOKING;
    }
    else if(it->second.status == CLIENT_CACHING)
    {
		it->second.status = CLIENT_REVOKING;
		it->second.isRevoke = true;
        //tprintf("revoke: id=%s, lid=%llu  CLIENT_CACHING\n",id.c_str(), lid );

        ret = send_msg(lock_protocol::release, lid, id);
		it->second.status = CLIENT_NONE;
        pthread_cond_broadcast(&it->second.cond);
        it->second.isRevoke = false;
    }else
    {
    	it->second.isRevoke = true;
    }

    //tprintf("revoke_handler finish: id=%s, lid=%llu status=%d\n",id.c_str(), lid ,it->second.status);
    pthread_mutex_unlock(&mapLock);
    return ret;

}




rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid,
                                 int &r)
{
    int ret = rlock_protocol::OK;
    pthread_mutex_lock(&mapLock);

    std::map<lock_protocol::lockid_t, client_lockInfo>::iterator it = locks.find(lid);
       //tprintf("retry_handler start: id=%s, lid=%llu status = %d\n",id.c_str(), lid ,it->second.status);
    //If lock doesn't exist, report error
      if(r == 1){
        //tprintf("retry_handler, id=%s, lid=%llu r=1\n",id.c_str(), lid);
        it->second.status = CLIENT_LOCKED;
      }
      else{
        //tprintf("retry_handler, id=%s, lid=%llu r=0\n",id.c_str(), lid);
        it->second.status = CLIENT_REVOKING;
      }
	//usleep(10000);
	it->second.isRetry = true;
    pthread_cond_signal(&it->second.retry);
	//tprintf("retry_handler finish: id=%s, lid=%llu status = %d\n",id.c_str(), lid ,it->second.status);
    pthread_mutex_unlock(&mapLock);
    return ret;
}


inline int  lock_client_cache::send_msg(int operation, lock_protocol::lockid_t lid,string id)
{
    int ret = lock_protocol::OK;
     pthread_mutex_unlock(&mapLock);
        int r;
    	ret = cl->call(lock_protocol::release, lid, id, r);
		//tprintf("send_msg, id=%s, lid=%llu , operation=%d\n",id.c_str(), lid, operation);
		if(lu != 0)
	    	lu->dorelease(lid);
    	//tprintf("send_msg release user done, id=%s, lid=%llu , operation=%d\n",id.c_str(), lid, operation);
    	pthread_mutex_lock(&mapLock);
    	//tprintf("send_msg done, id=%s, lid=%llu , operation=%d\n",id.c_str(), lid, operation);
     return ret;
}
