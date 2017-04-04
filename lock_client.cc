// RPC stubs for clients to talk to lock_server

#include "lock_client.h"
#include "rpc.h"
#include <arpa/inet.h>

#include <sstream>
#include <iostream>
#include <stdio.h>

lock_client::lock_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() < 0) {
    printf("lock_client: call bind\n");
  }
}

int
lock_client::stat(lock_protocol::lockid_t lid)
{
  int r;
  printf("lock_client: stat %llu\n",lid);
  lock_protocol::status ret = cl->call(lock_protocol::stat, cl->id(), lid, r);
  printf("lock_client: stat end\n");
  VERIFY (ret == lock_protocol::OK);
  return r;
}

lock_protocol::status
lock_client::acquire(lock_protocol::lockid_t lid)
{
	printf("lock_client: acquire %llu\n",lid);
	// Your lab4 code goes here
	int r;
	lock_protocol::status ret = cl->call(lock_protocol::acquire, cl->id(), lid, r);
	printf("lock_client: acquire end\n");
	VERIFY (ret == lock_protocol::OK);

	return ret;
}

lock_protocol::status
lock_client::release(lock_protocol::lockid_t lid)
{
	// Your lab4 code goes here
	int r;
	printf("lock_client: release %llu\n",lid);
	lock_protocol::status ret = cl->call(lock_protocol::release, cl->id(), lid, r);
	printf("lock_client: release end\n");
	VERIFY (ret == lock_protocol::OK);
	return ret;
}

