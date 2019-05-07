#ifndef _DEDUP_H
#define _DEDUP_H

#include <vector>
#include <string>
using namespace std;

#define SOTRATE_DIR "data"
#define META_DIR "metadata"
#define FILE_SUBFIX ".dedup"
#define IDS_DELIMIT ":"
#define BLOCK_SIZE 4096    // 4K


// error code
#define FILE_EXIST -100
#define FILE_NOT_EXIST -101

typedef struct block_mgt {
	uint32_t id;
	uint32_t ref;
	string checksum;
} Block_Mgt;

class dedup_mgt {
public:
	uint32_t block_uuid;    // block universal id
	string block_kv_name;    // block kv : md5(cont) | block_mgt
	string file_kv_name;     // file kv : filename | block_list
	map<string, Block_Mgt> block_kv;
	map<string, string> file_kv;
};

/*
struct dedup_mgt {
	uint32_t block_uuid;    // block universal id
	string block_kv_name;    // block kv : md5(cont) | block_mgt
	string file_kv_name;     // file kv : filename | block_list
	map<string, Block_Mgt> block_kv;
	map<string, string> file_kv;
};
*/

// tools
static void write_to_disk(const char *filename, const void *cont, int size);
static int read_from_disk(const char *filename, char *buf);
void init();
void store_meta();
void load_meta();
void split_str(const string& src, const string& separator, vector<string>& dest);

// func api
int add(const string filename);
int remove(const string filename);
int show(const string filename);
int list();
void usage();

#endif