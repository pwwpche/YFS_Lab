// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <assert.h>
#include "tprintf.h"
extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }

  pthread_mutex_init(&mapMutex, NULL);
}

// a demo to show how to use RPC
extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid,
		       extent_protocol::attr &attr)
{
    pthread_mutex_lock(&mapMutex);
    //tprintf("getattr begin: eid=%llu\n", eid);
    //printCache();
    extent_protocol::status ret = extent_protocol::OK;
    //If not loaded, load it into cache
    if(caches.find(eid) == caches.end())
    {
        //tprintf("getattr loading info: eid=%llu\n", eid);
        loadInfo(eid);
        //tprintf("getattr info loaded: eid=%llu\n", eid);
    }
    assert(caches.find(eid) != caches.end());
    //Get requested attr
    attr = caches[eid].attr;

    //tprintf("getattr end: eid=%llu\n", eid);
    //tprintf("type=%u, atime=%u, ctime=%u, size=%u\n",attr.type,  attr.atime, attr.ctime, attr.size);
    pthread_mutex_unlock(&mapMutex);
    return ret;

}

extent_protocol::status
extent_client::loadInfo(extent_protocol::extentid_t eid)
{
    //tprintf("loadInfo begin: eid=%llu\n", eid);
    //printCache();
    //pthread_mutex_lock(&mapMutex);
    extent_protocol::status ret = extent_protocol::OK;
    FileCache cache;
	usleep(1000);
    //Load attr
    extent_protocol::attr attr;
    while(1)
    {
        //tprintf("loadInfo start get attr: eid=%llu\n", eid);
        ret = cl->call(extent_protocol::getattr, eid, attr);
        //tprintf("loadInfo get attr end: eid=%llu, ret=%d\n", eid, ret);
        if(ret == extent_protocol::OK)
        {
            break;
        }
    }
    cache.attr = attr;

    //Load content
    std::string buf;
    while(1)
    {
        //tprintf("loadInfo start get buf: eid=%llu\n", eid);
        ret = cl->call(extent_protocol::get, eid, buf);
        ////tprintf("loadInfo get buf end: eid=%llu, ret=%d, buf=%s, len=%lu\n", eid, ret, buf.c_str(), buf.length());
        if (ret == extent_protocol::OK)
        {
            break;
        }
    }
    cache.content = buf;
    //tprintf("loadInfo insert into mem: eid=%llu\n", eid);
    //Store it into memory
    if(caches.find(eid) != caches.end())
    {
        caches[eid] = cache;
    }
    else
    {
        caches.insert(pair<extent_protocol::extentid_t, FileCache>(eid, cache));
    }

    //printCache();
    //tprintf("loadInfo end: eid=%llu\n", eid);
    //tprintf("type=%u, atime=%u, ctime=%u, size=%u\n",attr.type,  attr.atime, attr.ctime, attr.size);
    //pthread_mutex_unlock(&mapMutex);
    return ret;

}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
    pthread_mutex_lock(&mapMutex);
    extent_protocol::status ret = extent_protocol::OK;
    //tprintf("create begin: type=%u\n", type);
    //If id exists, then error(not legal to create an existed file)
    //if(caches.find(id) == caches.end())
    //{
        //tprintf("create call create: type=%u\n", type);
        ret = cl->call(extent_protocol::create, type, id);
        //tprintf("create call end: type=%u , ret=%d, id=%llu\n", type, ret, id);
        FileCache cache;
        //tprintf("create call getattr: type=%u\, id=%llu\n", type, id);
        ret = cl->call(extent_protocol::getattr, id, cache.attr);
        //tprintf("create call getattr: type=%u , ret=%d, id=%llu\n", type, ret, id);
        caches.insert(pair<extent_protocol::extentid_t, FileCache>(id, cache));
   // }
    caches[id].attr.atime = getCurrentTime();
    caches[id].attr.mtime = getCurrentTime();
    caches[id].attr.ctime = getCurrentTime();
    caches[id].attr.size = 0;

    pthread_mutex_unlock(&mapMutex);
    return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
    pthread_mutex_lock(&mapMutex);
    extent_protocol::status ret = extent_protocol::OK;
    //tprintf("get begin: eid=%llu\n", eid);
    //printCache();


    if(caches.find(eid) != caches.end())
    {
        //tprintf("get load from cache: eid=%llu\n", eid);
        buf = caches[eid].content;
        ////tprintf("get load from cache buf: eid=%llu\n , buf=%s, len=%lu\n", eid, buf.c_str(), buf.length());

    }
    else
    {
        //tprintf("get load from remote: eid=%llu\n", eid);
        ret = loadInfo(eid);
        //tprintf("get load from remote end: eid=%llu\n , ret=%d", eid, ret);
        assert(ret == extent_protocol::OK);
        buf = caches[eid].content;
        ////tprintf("get load from remote buf: eid=%llu\n , buf=%s, len=%lu\n", eid, buf.c_str(), buf.length());
    }
    caches[eid].attr.atime = getCurrentTime();
    //tprintf("get end: eid=%llu\n", eid);
    pthread_mutex_unlock(&mapMutex);
    return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
    pthread_mutex_lock(&mapMutex);
    extent_protocol::status ret = extent_protocol::OK;
    //tprintf("put begin: eid=%llu, buf=%s, len=%lu\n", eid, buf.c_str(), buf.length());

    //printCache();
    //ret = cl->call(extent_protocol::put, eid, buf, var);
    if(caches.find(eid) == caches.end())
    {
        //tprintf("put fetch from remote: eid=%llu\n", eid);
        //ret = cl->call(extent_protocol::get, eid, buf);
        ret = loadInfo(eid);
        assert(ret == extent_protocol::OK);
    }
    ////tprintf("put write to cache: eid=%llu, buf=%s len=%lu\n", eid, buf.c_str(), buf.length());
    //Write back to cache
    caches[eid].content = buf;
    caches[eid].dirty = DIRTY;
    caches[eid].attr.mtime = getCurrentTime();
    caches[eid].attr.ctime = getCurrentTime();
    caches[eid].attr.size = buf.length();

    //tprintf("put write cache content: eid=%llu, content=%s len=%lu\n", eid, caches[eid].content.c_str(), caches[eid].content.length());
    //printCache();
    //tprintf("put end: eid=%llu\n", eid);

    pthread_mutex_unlock(&mapMutex);
    return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
    pthread_mutex_lock(&mapMutex);
    extent_protocol::status ret = extent_protocol::OK;
    //tprintf("remove begin: eid=%llu\n", eid);
    //printCache();
    // Your lab3 code goes here

    //First remove from cache
    if(caches.find(eid) !=  caches.end())
    {
        //tprintf("remove found: eid=%llu\n", eid);
        caches.erase(eid);
    }
    //Remove file from server
    //ret = cl->call(extent_protocol::remove, eid, var);
    //printCache();
    //tprintf("remove end: eid=%llu\n", eid);
    pthread_mutex_unlock(&mapMutex);
    return ret;
}



extent_protocol::status
extent_client::flush(extent_protocol::extentid_t eid)
{
    pthread_mutex_lock(&mapMutex);
    extent_protocol::status ret = extent_protocol::OK;
    //tprintf("flush begin: eid=%llu\n", eid);
    //printCache();
    int var = 0;
    //If not in cache, then it was deleted
    if(caches.find(eid) == caches.end())
    {
        if(eid < 9999){
            ret = cl->call(extent_protocol::remove, eid, var);
        }
    }
    else
    {
        //Already marked as dirty, so write it back.
        if(eid < 9999)
        {
            if(caches[eid].dirty == DIRTY)
            {
                ret = cl->call(extent_protocol::put, eid, caches[eid].content, var);
                caches.erase(eid);
            }
        }

    }
    //printCache();
    //tprintf("flush end: eid=%llu\n", eid);
    pthread_mutex_unlock(&mapMutex);
    return ret;
}


void extent_client::printCache(){

    //tprintf("=======cache content====\n");
    map<extent_protocol::extentid_t, FileCache>::iterator it;

    for(it = caches.begin() ; it != caches.end() ; it++)
    {
        //tprintf("eid=%llu, buf=%s\n",it->first, (it->second).content.c_str() );
    }
    //tprintf("========================\n");
}

unsigned int extent_client::getCurrentTime()
{
	struct timeval start1;
	gettimeofday(&start1,0);
 	return start1.tv_sec * 1000 + start1.tv_usec;
}
