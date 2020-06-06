#include <cstdlib>
#include <cstdio>
#include <string>
#include <cstdint>
#include <iostream>
#include <iomanip>
#include <cassert>
#include <arpa/inet.h>
#include "hiredis.h"

#define MAX_FRAMES 10000

static uint32_t frames[MAX_FRAMES];
static uint32_t toggles[MAX_FRAMES];

#define SERIALIZE_TO_NET(arr, n) for (int i=0;i < n;i++) arr[i] = htonl(arr[i]);

using namespace std;

int AddSequences(redisContext *c, const string &key, const int n_sequences){
	int count = 0;
	for (int i=0;i < n_sequences;i++){
		int n_frames = rand()%MAX_FRAMES;
		for (int j=0;j < n_frames;j++){
			frames[j] = rand();
		}

		string descr = "Sequence #" + to_string(count);
		SERIALIZE_TO_NET(frames, n_frames);
	
		redisReply *reply = (redisReply*)redisCommand(c, "auscout.addtrack %s %b %s", key.c_str(), (void*)frames,
												  n_frames*sizeof(uint32_t), descr.c_str());
		assert(reply != NULL);
		assert(reply->type == REDIS_REPLY_INTEGER);
		count++;
		freeReplyObject(reply);
	}

	return count;
}

long long AddUniqueSequence(redisContext *c, const string &key, const int len){
	string descr = "mysequence";
	uint32_t n = 0;
	for (int i=0;i<len;i++){
		n += 100;
		frames[i] = n;
	}

	SERIALIZE_TO_NET(frames, len);

	redisReply *reply = (redisReply*)redisCommand(c, "auscout.addtrack %s %b %s", key.c_str(),
												  (void*)frames, len*sizeof(uint32_t), descr.c_str());

	assert(reply != NULL);
	assert(reply->type == REDIS_REPLY_INTEGER);
	long long id = reply->integer;
	freeReplyObject(reply);
	return id;
}

void QuerySequence(redisContext *c, const string &key){
	const double threshold = 0.80;
	const int n_frames = 500;
	uint32_t val = 2300;
	for (int i=0;i < n_frames;i++){
		toggles[i] = 0;
		frames[i] = val;
		val += 100;
	}

	SERIALIZE_TO_NET(frames, n_frames);
	SERIALIZE_TO_NET(toggles, n_frames);

	redisReply *reply = (redisReply*)redisCommand(c, "auscout.lookup %s %b %b %f", key.c_str(),
									  (void*)frames, n_frames*sizeof(uint32_t),
									  (void*)toggles, n_frames*sizeof(uint32_t), threshold);

	assert(reply != NULL);
	assert(reply->type == REDIS_REPLY_ARRAY);
	assert(reply->elements == 1);
	assert(reply->element[0]->type == REDIS_REPLY_ARRAY);

	redisReply *subreply = reply->element[0];
	assert(subreply->elements == 4);
	assert(subreply->element[2]->integer == 22);

	cout << "  descr = " << subreply->element[0]->str << endl;
	cout << "  id = " << subreply->element[1]->integer << endl;
	cout << "  pos = " << subreply->element[2]->integer << endl;
	cout << "  score = " << subreply->element[3]->str << endl;

	freeReplyObject(reply);
	return;
}

void DeleteSequence(redisContext *c, const string &key, const long long id){
	redisReply *reply = (redisReply*)redisCommand(c, "auscout.del %s %lld", key.c_str(), id);
	assert(reply != NULL);
	assert(reply->type == REDIS_REPLY_STATUS);
	freeReplyObject(reply);
	return;
}


long long GetCount(redisContext *c, const string &key){
	redisReply *reply = (redisReply*)redisCommand(c, "auscout.count %s", key.c_str());
	assert(reply != NULL);
	assert(reply->type == REDIS_REPLY_INTEGER);
	long long cnt = reply->integer;
	freeReplyObject(reply);
	return cnt;
}

long long GetSize(redisContext *c, const string &key){
	redisReply *reply = (redisReply*)redisCommand(c, "auscout.size %s", key.c_str());
	assert(reply != NULL);
	assert(reply->type == REDIS_REPLY_INTEGER);
	long long sz = reply->integer;
	freeReplyObject(reply);
	return sz;
}


void DeleteKey(redisContext *c, const string &key){
	redisReply *reply = (redisReply*)redisCommand(c, "auscout.delkey %s", key.c_str());
	assert(reply != NULL);
	assert(reply->type == REDIS_REPLY_STATUS);
	freeReplyObject(reply);
	return;
}


int main(int argc, char **argv){

	redisContext *c = redisConnect("localhost", 6379);
	assert(c != NULL);
	
	string key = "mytests";

	cout << "Add sequences" << endl;
	for (int i=0;i < 10;i++){
		int n = AddSequences(c, key, 100);
		assert(n == 100);
	}

	long long total = GetCount(c, key);
	assert(total == 1000);

	long long sz  = GetSize(c, key);
	assert(sz > 0);

	
	cout << "Add unique sequence" << endl;
	long long id = AddUniqueSequence(c, key, 5000);

	long long total2 = GetCount(c, key);
	assert(total2 == total + 1);

	QuerySequence(c, key);

	cout << "Delete unique sequence" << endl;
	DeleteSequence(c, key, id);

	long long total3 = GetCount(c, key);
	assert(total3 == total);

	DeleteKey(c, key);

	int cnt = GetCount(c, key);
	assert(cnt == 0);
	
	cout << "Done." << endl;
	redisFree(c);
	
	return 0;
}
