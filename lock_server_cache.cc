// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"
#include <assert.h>
#include <unistd.h>
lock_server_cache::lock_server_cache()
{
    pthread_mutex_init(&mapLock, 0);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id,
                               int &)
{
    lock_protocol::status ret = lock_protocol::RETRY;
    pthread_mutex_lock(&mapLock);

    //
    map< lock_protocol::lockid_t, server_lockInfo >::iterator it = locks.find(lid);
    if(it == locks.end())
    {
        //Create new lock
        server_lockInfo newLock;
        locks.insert(pair< lock_protocol::lockid_t, server_lockInfo >(lid, newLock) );
        it = locks.find(lid);
    }

    //
    unsigned pos = 0;;
    switch((it->second).status)
    {
        case STATUS_FREE:
            (it->second).owner = id;
            (it->second).status = STATUS_LOCKED;
            ret = lock_protocol::OK;
            //tprintf("owner %s is getting %llu\n", id.c_str(), lid);
            break;
        case STATUS_LOCKED:
            //Discard owner's request
            /*
            if(id == (it->second).owner)
            {
                //tprintf("owner %s is requiring %llu\n", id.c_str(), lid);
                ret = lock_protocol::RETRY;
                break;
            }
            */
            //tprintf("user %s is requiring %llu\n", id.c_str(), lid);
            //Looking up in the waiting list
            for(pos = 0 ; pos < (it->second).waiting.size() ; pos++)
            {
                if((it->second).waiting[pos] == id)
                {
                    ret = lock_protocol::RETRY;
                    break;
                }
            }
            //Request not exist, add to waiting list

            (it->second).waiting.push_back(id);


            //Revoke lock from owner

            announce_to_client(rlock_protocol::revoke, lid, (it->second).owner);
            ret = lock_protocol::RETRY;
            break;
    }
    //tprintf("leaving acquire(), id=%s, lid=%llu, ret=0x%x\n", id.c_str(), lid, ret);
    pthread_mutex_unlock(&mapLock);
    return ret;
}

int
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id,
         int &r)
{
    pthread_mutex_lock(&mapLock);
    int ret = lock_protocol::OK;
    map< lock_protocol::lockid_t, server_lockInfo >::iterator it = locks.find(lid);
    if(it == locks.end())
    {
        //tprintf("release error, lid %llu not found by id=%s\n", lid, id.c_str());

    }
    else
    {
        switch((it->second).status)
        {
            case STATUS_LOCKED:
                //The owner himself release the lock, just return ok
                if(id != (it->second).owner)
                {
                    //tprintf("release error, lid %llu id=%s, owner=%s\n", lid, id.c_str(), (it->second).owner.c_str());
                    ret = lock_protocol::RPCERR;
                    it->second.revoked = false;
                    break;
                }
                //No one is waiting.
                if((it->second).waiting.size() == 0)
                {
                    (it->second).owner = "";
                    (it->second).status = STATUS_FREE;
                    break;
                }
                //Announce next user to retry
                (it->second).owner = (it->second).waiting.front();
                (it->second).waiting.pop_front();
                ret = announce_to_client(rlock_protocol::retry, lid, (it->second).owner);
                if(it->second.waiting.size() > 0)
                {
                    ret = announce_to_client(rlock_protocol::revoke, lid, (it->second).owner);
                }
                break;
            default:
                break;
        }
    }

    pthread_mutex_unlock(&mapLock);
    return ret;
}



int lock_server_cache::announce_to_client(int operation, lock_protocol::lockid_t lid, string id)
{
        lock_protocol::status ret = lock_protocol::OK;
        while(1){
            int r;
            handle h(id);
            rpcc* cl = h.safebind();


            if (cl) {
                //tprintf("announce_to_client, lid=%llu,id=%s, operation =0x%x\n ",lid, id.c_str(), operation);
                pthread_mutex_unlock(&mapLock);
                ret = cl->call(operation, lid, r);
                pthread_mutex_lock(&mapLock);
                break;
            }
            if (!cl || ret != rlock_protocol::OK) {
                //tprintf("announce_to_client, lid=%llu,id=%s, operation =0x%x\n ",lid, id.c_str(), operation);
                if(!cl)
                {
                    //tprintf("announce_to_client, lid=%llu,id=%s, operation =0x%x cl error\n",lid, id.c_str(), operation);
                }
                else
                {
                    //tprintf("announce_to_client, lid=%llu,id=%s, operation =0x%x ret err\n ",lid, id.c_str(), operation);
                }
            }
        }
        //tprintf("announce_to_client, lid=%llu,id=%s, operation =0x%x ret=0x%x\n ",lid, id.c_str(), operation, ret);
        return ret;

}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
    //tprintf("stat request\n");
    r = nacquire;
    return lock_protocol::OK;
}

