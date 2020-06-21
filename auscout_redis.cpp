#include <cstdlib>
#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <ctime>
#include <chrono>
#include <arpa/inet.h>
#include "redismodule.h"

using namespace std;

#define AUSCOUT_ENCODING_VERSION 0
#define LOOKUP_ENTRIES_PER_FRAME_LIMIT 10
#define LOOKUP_BLOCK  100
#define LOOKUP_STEPS 16

static RedisModuleType *ASIndexType;

const char *descr_field = "descr";

typedef struct entry_t {
	entry_t *prev, *next, *succ;
	int64_t id;
	uint32_t pos, hash_value;
} Entry;

typedef struct entry_list_t {
	entry_t *head;
	uint32_t length;
} EntryList;

typedef struct as_index_t {
	RedisModuleDict *hash_dict; // frame / entry's
	RedisModuleDict *id_dict;   // id / entry's
	uint64_t n_entries;
} ASIndex;

typedef struct tracker_t {
	int start_index, last_index, pos, count;
} TrackerId;

typedef struct found_t {
	int64_t id;
	int64_t pos;
	double cs;
} FoundId;

/*------------------- Aux. functions --------------------------------*/

ASIndex* GetIndex(RedisModuleCtx *ctx, RedisModuleString *keystr){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keystr, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(key);
	if (keytype == REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_CloseKey(key);
		return NULL;
	}
	if (RedisModule_ModuleTypeGetType(key) != ASIndexType){
		RedisModule_CloseKey(key);
		throw -1;
	}

	ASIndex *index = (ASIndex*)RedisModule_ModuleTypeGetValue(key);
	RedisModule_CloseKey(key);
	return index;
}

ASIndex* CreateIndex(RedisModuleCtx *ctx, RedisModuleString *keystr){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keystr, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(key);
	if (keytype != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != ASIndexType){
		RedisModule_CloseKey(key);
		throw -1;
	}

	ASIndex *index = NULL;
	if (keytype == REDISMODULE_KEYTYPE_EMPTY){
		index = (ASIndex*)RedisModule_Calloc(1, sizeof(ASIndex));
		index->hash_dict = RedisModule_CreateDict(NULL);
		index->id_dict = RedisModule_CreateDict(NULL);
		index->n_entries = 0;
		RedisModule_ModuleTypeSetValue(key, ASIndexType, index);
	} else {
		index = (ASIndex*)RedisModule_ModuleTypeGetValue(key);
	}
	RedisModule_CloseKey(key);
	return index;
}


void add_entry(RedisModuleDict *hash_dict, uint32_t hashframe, Entry *e){
	EntryList *list = (EntryList*)RedisModule_DictGetC(hash_dict, &hashframe, sizeof(hashframe), NULL);
	if (list == NULL) {    // new entry
		list = (EntryList*)RedisModule_Calloc(1, sizeof(EntryList));
		list->head = e;
		list->length = 1;
		RedisModule_DictReplaceC(hash_dict, &hashframe, sizeof(hashframe), list);
		return;
	}
	e->next = list->head; // append entry to front of list
	list->head->prev = e;
	list->head = e;
	list->length++;
}

void remove_entry(RedisModuleDict *hash_dict, uint32_t hashframe, Entry *e){
	if (e->prev == NULL && e->next == NULL){ // sole entry, remove 
		EntryList *list = NULL;
		RedisModule_DictDelC(hash_dict, &hashframe, sizeof(hashframe), &list);
		RedisModule_Free(list);
		return;
	}

	int no_key;
	EntryList *list = (EntryList*)RedisModule_DictGetC(hash_dict, &hashframe, sizeof(hashframe), &no_key);
	if (list == NULL || no_key) return;

	if (e->prev == NULL){           // entry at start of list
		e->next->prev = NULL;
		list->head = e->next;
	} else if (e->next == NULL){   // entry at end of list
		e->prev->next = NULL;
	} else {                       // entry in middle of list
		e->next->prev = e->prev;
		e->prev->next = e->next;
	}
	list->length--;
}

int64_t get_next_id(RedisModuleCtx *ctx, RedisModuleString *keystr){
	int64_t id = RedisModule_Milliseconds() << 32;

	id |= (int64_t)(rand() & 0xffff0000);

	string key = RedisModule_StringPtrLen(keystr, NULL);
	key += ":counter";

	RedisModuleCallReply *reply = RedisModule_Call(ctx, "INCRBY", "cl", key.c_str(), 1);
	if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_INTEGER){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to generate unique Id");
		return REDISMODULE_ERR;
	}

	id |= (0x0000ffffULL & RedisModule_CallReplyInteger(reply));
	RedisModule_FreeCallReply(reply);
	return id;
}

/* return number of set bits in parameter value */
int bitcount(uint32_t toggle){
	uint32_t mask = 0x0001;
	int count = 0;
	while (toggle != 0){
		if (toggle & mask) count++;
		toggle >>= 1;
	}
	return count;
}

/* permute the bits in hashvalue as marked by the set bits in toggle */
/* put all permutations in vector                                    */
void get_candidates(uint32_t hashvalue, uint32_t toggle, vector<uint32_t> &candidates){
	int n_candidates = 0x01 << bitcount(toggle);
	candidates.push_back(hashvalue);
	for (int i=1;i < n_candidates;i++){
		uint32_t curr_value = hashvalue;
		uint32_t perms = i;
		uint32_t bitnum = 0x80000000;
		while (perms != 0 && bitnum != 0 && toggle != 0){
			while ((bitnum & toggle) == 0 && bitnum != 0) bitnum >>= 1;
			if ((perms & 0x00000001) != 0){
				curr_value ^= bitnum;
			}
			perms >>= 1;
			bitnum >>= 1;
		}
		candidates.push_back(curr_value);
	}
}

bool lookup_hashframe(RedisModuleCtx *ctx, const int current,
					  const double threshold,  RedisModuleDict *hash_dict,
					  uint32_t hashframe, map<int64_t, TrackerId>  &tracker, vector<FoundId> &results){

	bool found_match = false;
	EntryList *list = (EntryList*)RedisModule_DictGetC(hash_dict, &hashframe, sizeof(hashframe), NULL);
	if (list != NULL){
		Entry *e = list->head;
		int entry_count = 0;
		while (e != NULL && entry_count < LOOKUP_ENTRIES_PER_FRAME_LIMIT){
			if (tracker.count(e->id) > 0){
				// already being tracked 
				if (current <= tracker[e->id].last_index + LOOKUP_STEPS){
					// tracked id still in range 
					if (e->pos < tracker[e->id].pos) tracker[e->id].pos = e->pos;
					tracker[e->id].count++;
					tracker[e->id].last_index = current;
				} 

				// check if count is above threshold, and if so, add to results  
				int window_length = tracker[e->id].last_index - tracker[e->id].start_index + 1;
				if (window_length >= LOOKUP_BLOCK){
					double cs = (double)tracker[e->id].count/(double)window_length;
					if (cs >= threshold){
						results.push_back({.id = e->id, .pos = tracker[e->id].pos, .cs = cs});
						found_match = true;
						tracker.erase(e->id);
						return found_match;
					}
				}

				if (current > tracker[e->id].last_index + LOOKUP_STEPS){
					// tracked id falls out of range, reset starting index 
					tracker[e->id].start_index = current;
					tracker[e->id].last_index = current;
					tracker[e->id].pos = e->pos;
					tracker[e->id].count = 1;
				}

			} else {
				// id not being tracked, start tracking
				tracker[e->id] = {.start_index = current,
								  .last_index = current,
								  .pos = (int)e->pos,
								  .count = 1 };
			}
			
			e = e->next;
			entry_count++;
		}
		
	}

	return found_match;
}

/* retrieve a descr field stored in keystr+id hash redis datatype */
RedisModuleString* GetDescriptionField(RedisModuleCtx *ctx, RedisModuleString *keystr, long long id){
	string idstr = RedisModule_StringPtrLen(keystr, NULL);
	idstr += ":" + to_string(id);

	RedisModuleString *keyidstr = RedisModule_CreateString(ctx, idstr.c_str(), idstr.length());
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keyidstr, REDISMODULE_READ);
	if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(key);
		return NULL;
	}

	RedisModuleString *descr = NULL;
	RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, descr_field, &descr, NULL);
	RedisModule_CloseKey(key);
	return descr;
}

/* set a descr field for a keystr+id redis hash data type */ 
void SetDescriptionField(RedisModuleCtx *ctx, RedisModuleString *keystr, long long id, RedisModuleString *descr){
	string idstr = RedisModule_StringPtrLen(keystr, NULL);
	idstr += ":" + to_string(id);

	RedisModuleString *keyidstr = RedisModule_CreateString(ctx, idstr.c_str(), idstr.length());
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keyidstr, REDISMODULE_WRITE);
	if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_CloseKey(key);
		return;
	}

	RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, descr_field, descr, NULL);
	RedisModule_CloseKey(key);
}

void DeleteDescriptionField(RedisModuleCtx *ctx, RedisModuleString *keystr, long long id){
	string idstr = RedisModule_StringPtrLen(keystr, NULL);
	idstr += ":" + to_string(id);

	RedisModuleString *keyidstr = RedisModule_CreateString(ctx, idstr.c_str(), idstr.length());
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keyidstr, REDISMODULE_WRITE);
	if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(key);
		return;
	}

	RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, descr_field, REDISMODULE_HASH_DELETE, NULL);
	RedisModule_CloseKey(key);
}

void DeleteDescriptionKey(RedisModuleCtx *ctx, RedisModuleString *keystr, long long id){
	string idstr = RedisModule_StringPtrLen(keystr, NULL);
	idstr += ":" + to_string(id);

	RedisModuleString *keyidstr = RedisModule_CreateString(ctx, idstr.c_str(), idstr.length());
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keyidstr, REDISMODULE_WRITE);
	if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(key);
		return;
	}

	RedisModule_DeleteKey(key);
	RedisModule_CloseKey(key);
}

void DeleteCounterKey(RedisModuleCtx *ctx, RedisModuleString *keystr){
	string counterstr = RedisModule_StringPtrLen(keystr, NULL);
	counterstr += ":counter";

	RedisModuleString *keycounterstr = RedisModule_CreateString(ctx, counterstr.c_str(), counterstr.length());
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keycounterstr, REDISMODULE_WRITE);
	RedisModule_DeleteKey(key);
	RedisModule_CloseKey(key);
	return;
}

void DeleteKey(RedisModuleCtx *ctx, RedisModuleString *keystr){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keystr, REDISMODULE_WRITE);
	RedisModule_DeleteKey(key);
	RedisModule_CloseKey(key);
	return;
}

/* ------------------ Auscout type methods --------------------------*/

extern "C" void* ASIndexTypeRdbLoad(RedisModuleIO *rdb, int encver){
	if (encver != AUSCOUT_ENCODING_VERSION){
		RedisModule_LogIOError(rdb, "warning", "rdbload: unable to encode for encver %d", encver);
		return NULL;
	}

	ASIndex *index = (ASIndex*)RedisModule_Alloc(sizeof(ASIndex));
	index->hash_dict = RedisModule_CreateDict(NULL); 
	index->id_dict = RedisModule_CreateDict(NULL);
	index->n_entries = 0;

	uint64_t n_ids = RedisModule_LoadUnsigned(rdb);
	for (uint64_t i=0;i < n_ids;i++){

		int64_t id = RedisModule_LoadSigned(rdb);
		uint32_t n_frames = (int32_t)RedisModule_LoadUnsigned(rdb);
		index->n_entries += n_frames;

		EntryList *list = (EntryList*)RedisModule_Calloc(1, sizeof(EntryList));
		RedisModule_DictSetC(index->id_dict, &id, sizeof(id), list);
		index->n_entries += n_frames;

		Entry *curr = NULL;
		for (uint32_t j=0;j<n_frames;j++){
			if (j == 0){
				list->head = curr = (Entry*)RedisModule_Calloc(1, sizeof(Entry));
				list->length = n_frames;
			} else {
				curr->succ = (Entry*)RedisModule_Calloc(1, sizeof(Entry));
				curr = curr->succ;
			}
			curr->id = id;
			curr->hash_value = (uint32_t)RedisModule_LoadUnsigned(rdb);
			curr->pos = (uint32_t)RedisModule_LoadSigned(rdb);
			add_entry(index->hash_dict, curr->hash_value, curr);
		}
	}

	return index;
}

extern "C" void ASIndexTypeRdbSave(RedisModuleIO *rdb, void *value){
	ASIndex *index = (ASIndex*)value;
	RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(index->id_dict, "^", NULL, 0);
	unsigned char *dict_key = NULL;
	size_t keylen;
	EntryList *list = NULL;

	uint64_t n_ids = RedisModule_DictSize(index->id_dict);
	RedisModule_SaveUnsigned(rdb, n_ids);
	while ((dict_key = (unsigned char*)RedisModule_DictNextC(iter, &keylen, (void**)&list)) != NULL){
		uint64_t nframes = list->length;
		int64_t id = list->head->id;
		RedisModule_SaveSigned(rdb, id);
		RedisModule_SaveUnsigned(rdb, nframes);
		uint32_t i = 0;
		Entry *entry = list->head;
		while (entry != NULL && i < nframes){
			uint64_t tmphash = (uint64_t)entry->hash_value;
			int64_t pos = (int64_t)entry->pos;
			RedisModule_SaveUnsigned(rdb, tmphash);
			RedisModule_SaveSigned(rdb, pos);
			entry = entry->succ;
			i++;
		}
	}
}

extern "C" void ASIndexTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value){
	ASIndex *index = (ASIndex*)value;
	RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(index->id_dict, "^", NULL, 0);
	unsigned char *dict_key = NULL;
	size_t keylen;
	EntryList *list = NULL;

	vector<uint32_t> hashesforid;
	while ((dict_key = (unsigned char*)RedisModule_DictNextC(iter, &keylen, (void**)&list)) != NULL){
		Entry *entry = list->head;
		int64_t id = entry->id;
		while (entry != NULL){
			hashesforid.push_back(htonl(entry->hash_value));
			entry->succ;
		}
		
		RedisModule_EmitAOF(aof, "auscout.add", "sbl",
					 key, (unsigned char*)hashesforid.data(), hashesforid.size()*sizeof(uint32_t), id);
		hashesforid.clear();
	}
}

extern "C" void ASIndexTypeFree(void *value){
	ASIndex *index = (ASIndex*)value;
	RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(index->id_dict, "^", NULL, 0);
	unsigned char *dict_key = NULL;
	size_t keylen;
	EntryList *list = NULL;
	while ((dict_key = (unsigned char*)RedisModule_DictNextC(iter, &keylen, (void**)&list)) != NULL){
		Entry *entry = list->head;
		while (entry != NULL){
			Entry *tmp = entry;
			entry = entry->succ;
			remove_entry(index->hash_dict, tmp->hash_value, tmp);
			RedisModule_Free(tmp);
		}
		RedisModule_Free(list);
	}

	RedisModule_DictIteratorStop(iter);
	RedisModule_FreeDict(NULL, index->hash_dict);
	RedisModule_FreeDict(NULL, index->id_dict);
}

extern "C" size_t ASIndexTypeMemUsage(const void *value){
	ASIndex *index = (ASIndex*)value;
	uint64_t n_ids = RedisModule_DictSize(index->id_dict);
	uint64_t n_hashes = RedisModule_DictSize(index->hash_dict);
	size_t entries_sz = (index->n_entries)*sizeof(Entry);
	size_t list_sz = (n_ids + n_hashes)*sizeof(EntryList);
	size_t dict_sz = (n_ids + n_hashes)*sizeof(EntryList*);
	return entries_sz + list_sz + dict_sz;;
}

extern "C" void ASIndexTypeDigest(RedisModuleDigest *digest, void *value){
	REDISMODULE_NOT_USED(digest);
	REDISMODULE_NOT_USED(value);
}
/* -----------------DEBUGGING COMMANDS-----------------------------*/

/* list all entries by id, a list of hashvalues for each id */
/* ARGS: key */
extern "C" int AuscoutList_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 2) return RedisModule_WrongArity(ctx);

	ASIndex *index = NULL;
	try {
		index = GetIndex(ctx, argv[1]);
		if (index == NULL) {
			RedisModule_ReplyWithError(ctx, "ERR - no such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type.  Delete first.");
		return REDISMODULE_ERR;
	}

	RedisModule_Log(ctx, "debug", "ID List in key,  %s", RedisModule_StringPtrLen(argv[1], NULL));
	RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(index->id_dict, "^", NULL, 0);
	unsigned char *dict_key;
	size_t keylen;
	EntryList *list = NULL;
	long long count = 0;
	while ((dict_key = (unsigned char*)RedisModule_DictNextC(iter, &keylen, (void**)&list)) != NULL){
		Entry *entry = list->head;
		long long *idptr = (long long*)dict_key;
		RedisModule_Log(ctx, "debug", "(%d) keylen = %d, id = %lld no. entries = %lu",
						++count, keylen, *idptr, list->length);
		int index = 0;
		while (entry != NULL){
			RedisModule_Log(ctx, "debug", "    (%d) id = %lld, hashvalue = %lu, pos = %lu",
							++index, entry->id, entry->hash_value, entry->pos);
			entry = entry->succ;
		}
	}

	RedisModule_Log(ctx, "debug", "list done");
	RedisModule_DictIteratorStop(iter);

	RedisModule_ReplyWithLongLong(ctx, count);
	
	return REDISMODULE_OK;
}

/* list all hashvalues */
/* ARGS: key */
extern "C" int AuscoutIndex_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 2) return RedisModule_WrongArity(ctx);

	ASIndex *index = NULL;
	try {
		index = GetIndex(ctx, argv[1]);
		if (index == NULL) {
			RedisModule_ReplyWithError(ctx, "ERR - no such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type.  Delete first.");
		return REDISMODULE_ERR;
	}

	RedisModule_Log(ctx, "debug", "Hash List in key,  %s", RedisModule_StringPtrLen(argv[1], NULL));
	RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(index->hash_dict, "^", NULL, 0);
	unsigned char *dict_key;
	size_t keylen;
	EntryList *list = NULL;
	long long count = 0;
	while ((dict_key = (unsigned char*)RedisModule_DictNextC(iter, &keylen, (void**)&list)) != NULL){
		Entry *entry = list->head;
		uint32_t *hashptr = (uint32_t*)dict_key;
		RedisModule_Log(ctx, "debug", "(%d) keylen = %d, hash frame = %lu no. entries = %d",
						++count, keylen, *hashptr, list->length);
		int index=0;
		while (entry != NULL){
			RedisModule_Log(ctx, "debug", "    (%d) id = %lld, hash = %lu, pos = %lu",
							++index, entry->id, entry->hash_value, entry->pos);
			entry = entry->next;
		}
	}
	RedisModule_Log(ctx, "debug", "list done.");
	RedisModule_DictIteratorStop(iter);

	RedisModule_ReplyWithLongLong(ctx, count);
	return REDISMODULE_OK;
}

/* ------------------------------------------------------------------*/
/* ARGS: key hashstr [id]  */
int64_t auscoutadd_common(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModuleString *keystr = argv[1];
	RedisModuleString *hashstr = argv[2];

	long long id;
	if (argc == 3){
		id = get_next_id(ctx, argv[1]);
	} else {
		if (RedisModule_StringToLongLong(argv[3], &id) == REDISMODULE_ERR){
		    RedisModule_ReplyWithError(ctx, "ERR - Unable to parse id arg");
			throw -1;
		}
	}

	ASIndex *index = NULL;
	try {
		index = GetIndex(ctx, argv[1]);
		if (index == NULL) index = CreateIndex(ctx, argv[1]);
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type.  Delete first.");
		throw -1;
	}

	size_t len;
	uint32_t *data = (uint32_t*)RedisModule_StringPtrLen(hashstr, &len);
    uint32_t n_frames = len / sizeof(uint32_t);

	RedisModule_Log(ctx, "debug", "recieved %d hash frames", n_frames);

	EntryList *list = (EntryList*)RedisModule_Calloc(1, sizeof(EntryList));
	if (RedisModule_DictSetC(index->id_dict, &id, sizeof(id), list) == REDISMODULE_ERR){
		RedisModule_ReplyWithError(ctx, "ERR - id already exists");
		throw -1;
	}


	Entry *curr = NULL;
	uint32_t prev_frame = 0;
	for (uint32_t i=0;i < n_frames;i++){
		uint32_t curr_frame = ntohl(data[i]);
		if (curr_frame != prev_frame){
			Entry *e = (Entry*)RedisModule_Calloc(1, sizeof(Entry));
			if (list->head == NULL){
				list->head = curr = e;
			} else {
				curr->succ = e;
				curr = curr->succ;
			}
			curr->id = id;
			curr->hash_value = curr_frame;
			curr->pos = i;
			index->n_entries++;
			add_entry(index->hash_dict, curr->hash_value, curr);
			prev_frame = curr_frame;
			list->length++;
		}
	}

	return id;
}


/* ARGS: key hashbytestr [id] */
extern "C" int AuscoutAdd_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 3) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	int64_t id;
	try {
		id = auscoutadd_common(ctx, argv, argc);
	} catch (int &e){
		return REDISMODULE_ERR;
	}

	RedisModule_ReplyWithLongLong(ctx, id);

	if (RedisModule_Replicate(ctx, "auscout.add", "ssl", argv[1], argv[2], id) == REDISMODULE_ERR){
		RedisModule_Log(ctx, "warning", "WARN - Unable to replicate for id");
		return REDISMODULE_ERR;
	}
	
	return REDISMODULE_OK;
}

/* ARGS: key hashbytestr descr [id]  */
extern "C" int AuscoutAddWithDescr_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 4) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	RedisModuleString *descrstr = argv[3];

	int64_t id;
	try {
		if (argc == 4){
			id = auscoutadd_common(ctx, argv, argc-1);
		} else {
			argv[3] = argv[4];
			id = auscoutadd_common(ctx, argv, argc-1);
		}
	} catch (int &e){
		return REDISMODULE_ERR;
	}


	SetDescriptionField(ctx, argv[1], id,descrstr);

	RedisModule_ReplyWithLongLong(ctx, id);

	if (RedisModule_Replicate(ctx, "auscout.addtrack", "sssl", argv[1], argv[2], descrstr, id)
		== REDISMODULE_ERR){
		RedisModule_Log(ctx, "warning", "WARN - Unable to replicate for id");
		return REDISMODULE_ERR;
	}
	
	return REDISMODULE_OK;
}

/* ARGS: key id_value */
extern "C" int AuscoutDel_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 3) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	long long id;
	if (RedisModule_StringToLongLong(argv[2], &id) == REDISMODULE_ERR){
		RedisModule_ReplyWithError(ctx, "unable to parse id arg");
		return REDISMODULE_ERR;
	}
	
	ASIndex *index = NULL;
	try {
		index = GetIndex(ctx, argv[1]);
		if (index == NULL) {
			RedisModule_ReplyWithError(ctx, "ERR - no such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type.  Delete first.");
		return REDISMODULE_ERR;
	}

	RedisModule_Log(ctx, "debug", "delete %lld at key %s", id, RedisModule_StringPtrLen(argv[1], NULL));
	
	EntryList *list = NULL;
	if (RedisModule_DictDelC(index->id_dict, &id, sizeof(id), &list) == REDISMODULE_ERR){
		RedisModule_ReplyWithError(ctx, "no such id found");
		return REDISMODULE_ERR;
	}

	index->n_entries -= list->length;
	
	Entry *curr = list->head;
	while (curr != NULL){
		Entry *tmp = curr;
		curr = curr->succ;
		remove_entry(index->hash_dict, tmp->hash_value, tmp);
		RedisModule_Free(tmp);
	}

	long long n_dels = list->length;
	
	RedisModule_Free(list);

	DeleteDescriptionField(ctx, argv[1], id);

	RedisModule_ReplyWithLongLong(ctx, n_dels);
	RedisModule_ReplicateVerbatim(ctx);
	return REDISMODULE_OK;
}

/* ARGS: key hashbytestr togglebytestr [threshold] */
extern "C" int AuscoutLookup_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 4) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);
	
	RedisModuleString *keystr = argv[1];
	RedisModuleString *hashbytestr = argv[2];
	RedisModuleString *togglebytestr = argv[3];

	chrono::time_point<chrono::high_resolution_clock> start = chrono::high_resolution_clock::now();
	
	double threshold = 0.30;
	if (argc > 4){
		if (RedisModule_StringToDouble(argv[4], &threshold) == REDISMODULE_ERR){
			RedisModule_ReplyWithError(ctx, "ERR - unable to parse threshold parameter");
			return REDISMODULE_ERR;
		}
	}
	
	size_t len, len2;
	uint32_t *hasharray = (uint32_t*)RedisModule_StringPtrLen(hashbytestr, &len);
	uint32_t *togglesarray = (uint32_t*)RedisModule_StringPtrLen(togglebytestr, &len2);

	
	if (len < sizeof(uint32_t) || len2 < sizeof(uint32_t)){ // arrays must be at least one integer
		RedisModule_ReplyWithError(ctx, "insufficient length arrays");
		return REDISMODULE_ERR;
	}
	
	if (len != len2){
		RedisModule_ReplyWithError(ctx, "hash array must be equal to toggle array length");
		return REDISMODULE_ERR;
	}

	ASIndex *index = NULL;
	try {
		index = GetIndex(ctx, keystr);
		if (index == NULL) {
			RedisModule_ReplyWithError(ctx, "ERR - no such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type.  Delete first.");
		return REDISMODULE_ERR;
	}

	int  n_frames = len/sizeof(uint32_t);
	
	RedisModule_Log(ctx, "debug", "lookup - recieved %d frames - threshold %f", n_frames, threshold);
	
	map<int64_t, TrackerId> tracker;
	vector<FoundId> results;
	for (int i=0;i < n_frames;i++){
		uint32_t frame = ntohl(hasharray[i]);
		uint32_t toggle = ntohl(togglesarray[i]);
		
		vector<uint32_t> candidates;
		get_candidates(frame, toggle, candidates);
		for (uint32_t key : candidates){
			lookup_hashframe(ctx, i, threshold, index->hash_dict, key, tracker, results);
		}
		candidates.clear();

		if (results.size() > 0)
			break;
	}
	

	RedisModule_Log(ctx, "debug", "done looking up - found %d", results.size());

	long n_results = 0;
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	for (FoundId fnd : results){
		RedisModuleString *descr = GetDescriptionField(ctx, keystr, fnd.id);
		int n = (descr) ? 4 : 3;
		RedisModule_ReplyWithArray(ctx, n);
		if (descr) RedisModule_ReplyWithString(ctx, descr);
		RedisModule_ReplyWithLongLong(ctx, (long long)fnd.id);
		RedisModule_ReplyWithLongLong(ctx, (long long)fnd.pos);
		RedisModule_ReplyWithDouble(ctx, fnd.cs);
		n_results++;
	}
	RedisModule_ReplySetArrayLength(ctx, n_results);

	chrono::time_point<chrono::high_resolution_clock> end = chrono::high_resolution_clock::now();
	auto elapsed = chrono::duration_cast<chrono::microseconds>(end - start).count();
	unsigned int dur = elapsed;
	RedisModule_Log(ctx, "debug", "Lookup processed in %u microseconds", dur);
	
	return REDISMODULE_OK;
}

/* ARGS: key  */
extern "C" int AuscoutSize_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 2) return RedisModule_WrongArity(ctx);

	ASIndex *index = NULL;
	long long n_entries = 0;
	try {
		index = GetIndex(ctx, argv[1]);
		if (index != NULL) n_entries = index->n_entries;
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type.  Delete first.");
		return REDISMODULE_ERR;
	}

	RedisModule_ReplyWithLongLong(ctx, n_entries);
	return REDISMODULE_OK;
}

/* ARGS: key */
extern "C" int AuscoutCount_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 2) return RedisModule_WrongArity(ctx);

	ASIndex *index = NULL;
	long long n_ids = 0;
	try {
		index = GetIndex(ctx, argv[1]);
		if (index != NULL) n_ids = RedisModule_DictSize(index->id_dict);
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type.  Delete first.");
		return REDISMODULE_ERR;
	}

	RedisModule_ReplyWithLongLong(ctx, n_ids);
	return REDISMODULE_OK;
}

/* ARGS: key */
extern "C" int AuscoutDelKey_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 2) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);

	ASIndex *index = NULL;
	try {
		index = GetIndex(ctx, argv[1]);
		if (index == NULL){
			RedisModule_ReplyWithError(ctx, "No such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type.  Delete first.");
		return REDISMODULE_ERR;
	}

	RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(index->id_dict, "^", NULL, 0);
	long long *dict_key = NULL;
	size_t keylen;
	EntryList *list = NULL;
	while ((dict_key = (long long*)RedisModule_DictNextC(iter, &keylen, (void**)&list)) != NULL){
		DeleteDescriptionKey(ctx, argv[1], *dict_key);
	}

	RedisModule_DictIteratorStop(iter);

	DeleteCounterKey(ctx, argv[1]);
	DeleteKey(ctx, argv[1]);
	
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	return REDISMODULE_OK;
}

extern "C" int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){

	if (RedisModule_Init(ctx, "auscout", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR){
		RedisModule_Log(ctx, "warning", "unable to init module");
		return REDISMODULE_ERR;
	}

	RedisModule_Log(ctx, "debug", "init auscout module");
	
	RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
	                             .rdb_load = ASIndexTypeRdbLoad,
	                             .rdb_save = ASIndexTypeRdbSave,
	                             .aof_rewrite = ASIndexTypeAofRewrite,
	                             .mem_usage = ASIndexTypeMemUsage,
	                             .digest = ASIndexTypeDigest,
	                             .free = ASIndexTypeFree};

	ASIndexType = RedisModule_CreateDataType(ctx, "AuScoutDS", AUSCOUT_ENCODING_VERSION, &tm);
	if (ASIndexType == NULL)
		return REDISMODULE_ERR;

	RedisModule_Log(ctx, "debug", "create AsIndexType datatype");
	
	if (RedisModule_CreateCommand(ctx, "auscout.add", AuscoutAdd_RedisCmd,
								  "write deny-oom", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "auscout.addtrack", AuscoutAddWithDescr_RedisCmd,
								  "write deny-oom", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if (RedisModule_CreateCommand(ctx, "auscout.del", AuscoutDel_RedisCmd,
								  "write deny-oom", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "auscout.lookup", AuscoutLookup_RedisCmd,
								  "readonly deny-oom", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "auscout.size", AuscoutSize_RedisCmd,
								  "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "auscout.count", AuscoutCount_RedisCmd,
								  "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;


	if (RedisModule_CreateCommand(ctx, "auscout.delkey", AuscoutDelKey_RedisCmd,
								  "write deny-oom", 1, -1, 1) == REDISMODULE_ERR){
		return REDISMODULE_ERR;
	}

	if (RedisModule_CreateCommand(ctx, "auscout.list", AuscoutList_RedisCmd,
								  "readonly", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "auscout.index", AuscoutIndex_RedisCmd,
								  "readonly", 1, -1, 1) == REDISMODULE_ERR);
	
	return REDISMODULE_OK;
}
