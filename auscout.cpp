#include <cstdlib>
#include <cstdio>
#include <string>
#include <cstdint>
#include <algorithm>
#include <iostream>
#include <iomanip>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <arpa/inet.h>
#include "hiredis.h"

#define SERIALIZE_TO_NET(arr, n) for (int i=0;i < n;i++) arr[i] = htonl(arr[i]);

extern "C" {
#include <audiodata.h>
#include <AudioHash.h>
}

using namespace std;
namespace fs = boost::filesystem;
namespace po = boost::program_options;
namespace ag = boost::algorithm;

struct Args {
	string cmd, host, key;
	fs::path dirname;
	int port, t, sr, n_secs;
	long long id_value;
	float threshold;
};

Args ParseOptions(int argc, char **argv){
	Args args;
	po::options_description descr("Auscout Options");
	try {
		descr.add_options()
			("help,h", "produce help message")
			("key,k", po::value<string>(&args.key)->required(), "redis key string")
			("dir,d", po::value<fs::path>(&args.dirname)->required(), "directory of audio files to process")
			("cmd,c", po::value<string>(&args.cmd)->required(), "command: add, del, lookup or help")
			("server,s", po::value<string>(&args.host)->default_value("localhost"),
			   "redis server hostname or unix domain socket path - e.g. localhost or 127.0.0.1")
			("port,p", po::value<int>(&args.port)->default_value(6379), "redis server port")
			("toggles,g", po::value<int>(&args.t)->default_value(4), "query parameter - 0 to 8")
			("sr", po::value<int>(&args.sr)->default_value(6000), "sample rate - e.g. 6000")
			("nsecs,n", po::value<int>(&args.n_secs)->default_value(0),
			 "number seconds to process from  each file - deafult value 0 for whole file")
			("threshold,t", po::value<float>(&args.threshold)->default_value(0.25),
			 "query threshold - e.g. 0.10 (0,1.0)")
			("id,i", po::value<long long>(&args.id_value), "id value for delete");
			
		po::positional_options_description pd;
		pd.add("cmd", 1);
		
		po::variables_map vm;
		po::store(po::command_line_parser(argc, argv).options(descr).positional(pd).run(), vm);
		
		if (vm.count("help") || args.cmd == "help"){
			cout << descr << endl;
			exit(0);
		}

		po::notify(vm);
	} catch (const po::error &ex){
		cout << descr << endl;
		exit(0);
	}
	
	return args;
}

int SubmitFiles(redisContext *c, const string &keystr, const fs::path &dirname, const int sr){
	fs::directory_iterator dir(dirname), end;

	int count = 0;
	int err;
	int len;
	AudioMetaData mdata;
	AudioHashStInfo info;
	AudioHash hash;
	ph_init_hashst(&info);

	const int buflen = 1 << 25;
	float *sigbuf = new float[buflen];
	for ( ; dir != end; ++dir){
		if (fs::is_regular_file(dir->status())){
			string path = dir->path().string();
			len = buflen;
			float *buf = readaudio(path.c_str(), sr, 0, sigbuf, &len, &mdata, &err);

			string filename = dir->path().filename().string();

			if (buf != NULL){

				ph_audiohash(buf, len, &hash, &info, 0, sr);

				cout << "(" << count << ") " << filename
					 << " samples - " << len << " frames " << hash.nbhashes << endl;
			
		
				uint32_t *hasharray = hash.hasharray;
				int n_hashes = hash.nbhashes;

				SERIALIZE_TO_NET(hasharray, n_hashes);
				
				string filename = dir->path().filename().string();

				redisReply *reply = (redisReply*)redisCommand(c, "auscout.addtrack %s %b %s",
												 keystr.c_str(), (void*)hasharray,
												 n_hashes*sizeof(uint32_t), filename.c_str());

				free_mdata(&mdata);
				ph_free_hash(&hash);
				
				if (reply && reply->type == REDIS_REPLY_INTEGER){
					cout << "=> added with id = " << reply->integer << endl;
					count++;
				} else if (reply && reply->type == REDIS_REPLY_ERROR){
					cerr << "=> error - " << reply->str << endl;
				} else {
					cerr << "Disconnected" << endl;
					break;
				}

			} else {
				cerr << "bad file" << endl;
			}
			
		}
	}

	delete sigbuf;
	ph_free_hashst(&info);
	return count;
}

int DeleteId(redisContext *c, const string &keystr, long long id){
	cout << "delete " << id << endl;
	redisReply *reply = (redisReply*)redisCommand(c, "auscout.del %s %lld", keystr.c_str(), id);
	if (reply && reply->type == REDIS_REPLY_STRING){
		cout << reply->str << endl;
	} else  if (reply && reply->type == REDIS_REPLY_ERROR){
		cout << "error: " << reply->str << endl;
	} else {
		cerr << "error" << endl;
	}

	return 1;
}


/*  
 *	reply arrays: description, id, position, confidence score
*/
void ProcessSubReply(redisReply *reply, const int sr){

	if (reply->type == REDIS_REPLY_ARRAY){
		float nsecs = ph_get_offset_secs(sr, reply->element[2]->integer);
		if (reply->elements == 4){
			cout << "descr: " << reply->element[0]->str << endl;
			cout << "id   : " << reply->element[1]->integer << endl;
			cout << "secs : " << nsecs << endl << " (pos = " << reply->element[2]->integer << ")" << endl;;
			cout << "cs   : " << reply->element[3]->str << endl;
		} else if (reply->elements == 3){
			cout << "id   : " << reply->element[0]->integer << endl;
			cout << "secs : " << nsecs << endl << " (pos = " << reply->element[2]->integer << ")" << endl;;
			cout << "cs   : " << reply->element[2]->str << endl;
		}
	}
}


int QueryFiles(redisContext *c, const string &key, const fs::path &dirname,
			   const int sr, const int p, const int n_secs, const float threshold){
	fs::directory_iterator dir(dirname), end;

	int count = 0;
	int err;
	int len;
	AudioMetaData mdata;
	AudioHashStInfo info;
	AudioHash hash;
	ph_init_hashst(&info);

	const int buflen = 1 << 18;
	float *sigbuf = new float[buflen];
	for ( ; dir != end; ++dir){
		if (fs::is_regular_file(dir->status())){
			string file = dir->path().string();
			len = buflen;
			float *buf = readaudio(file.c_str(), sr, n_secs, sigbuf, &len, &mdata, &err);
			if (buf != NULL){

				ph_audiohash(buf, len, &hash, &info, p, sr);

				cout << "lookup: " << file << " samples " << len << " nhashes " << hash.nbhashes << endl;
				
				uint32_t *hasharray = hash.hasharray;
				uint32_t *toggles = hash.toggles;
				int n_hashes = hash.nbhashes;

				if (toggles == NULL) {
					toggles = new uint32_t[n_hashes];
					for (int i=0;i<n_hashes;i++) toggles[i] = 0;
				}
				
				SERIALIZE_TO_NET(hasharray, n_hashes);
				SERIALIZE_TO_NET(toggles, n_hashes);
				
				redisReply *reply = (redisReply*)redisCommand(c, "auscout.lookup %s %b %b %f", key.c_str(),
															  (void*)hasharray, n_hashes*sizeof(uint32_t),
															  (void*)toggles, n_hashes*sizeof(uint32_t),
															  threshold);
											   
				free_mdata(&mdata);
				ph_free_hash(&hash);
				
				if (reply && reply->type == REDIS_REPLY_ARRAY){
					for (int i=0;i<reply->elements;i++){
						ProcessSubReply(reply->element[i], sr);
					}
					freeReplyObject(reply);
					count++;
				} else if (reply && reply->type == REDIS_REPLY_STRING){
					cout << "=> " << reply->str << endl;
				} else if (reply && reply->type == REDIS_REPLY_ERROR){
					cerr << "=> error - " << reply->str << endl;
					freeReplyObject(reply);
				} else {
					cerr << "Disconnected" << endl;
					break;
				}
			} else {
				cerr << "error: " << err << endl;
			}
		}
	}

	delete sigbuf;
	ph_free_hashst(&info);
	return count;
}

void print_header(){
	cout << endl << "---------Redis AudioScout Client----------" << endl << endl;
}

int main(int argc, char **argv){
	print_header();

	Args args = ParseOptions(argc, argv);

	cout << endl << "Connect to " << args.host << ":" << args.port << endl;
	
	redisContext *c = redisConnect(args.host.c_str(), args.port);
	if (c == NULL || c->err){
		if (c)  cerr << "Unable to connect: " << c->errstr << endl;
		else    cerr << "Unable to connect to server" << endl;
		exit(0);
	}

	cout << "sample rate = " << args.sr << endl;
	
	int n = 0;
	if (args.cmd == "add"){
		cout << "Add files in " << args.dirname << " to key, " << args.key << endl;
		n = SubmitFiles(c, args.key, args.dirname, args.sr);
	} else if (args.cmd == "lookup"){
		cout << "Lookup files in " << args.dirname << " from key, " << args.key << endl;
		cout << "(  threshold = " << args.threshold << ")" << endl;
		n = QueryFiles(c, args.key, args.dirname, args.sr, args.t, args.n_secs, args.threshold);
	} else if (args.cmd == "del"){
		cout << "Delete id = " << args.id_value << endl;
		DeleteId(c, args.key, args.id_value);
	} else {
		cerr << "Unknown command - " << args.cmd << endl;
	}

	cout << "Total " << n << " files processed." << endl;
	cout << "Done." << endl;
	
	redisFree(c);
	return 0;
}
