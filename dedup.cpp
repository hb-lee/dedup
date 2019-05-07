#include <iostream>
#include <fstream>
#include <stdint.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <cstring>
#include <string>
#include <map>
#include "dedup.h"
#include "md5.h"

class dedup_mgt dedup;

static void write_to_disk(const char *filename, const void *cont, int size)
{
	string storage_name;
	string name(filename);
	storage_name = string(SOTRATE_DIR) + "/" + name;
	int fd = open(storage_name.data(), O_WRONLY|O_CREAT);
	write(fd, cont, size);
	close(fd);
}

static int read_from_disk(const char *filename, char *buf)
{
	int read_size = 0;
	string storage_name;
	string name(filename);
	storage_name = string(SOTRATE_DIR) + "/" + name;
	int fd = open(storage_name.data(), O_RDONLY);
	read_size = read(fd, buf, BLOCK_SIZE);
	close(fd);
	return read_size;
}

void init()
{
	cout << "Init ..."<<endl;
	string dir;
	dir = SOTRATE_DIR;
	if (access(dir.data(), 0) != 0)
	{
		mkdir(dir.data(), 0700);
	}
	dir = META_DIR;
	if (access(dir.data(), 0) != 0)
	{
		mkdir(dir.data(), 0700);
	}
	dedup.block_uuid = 0;
	dedup.block_kv_name = dir + string("/blockkv.md");
	if (access(dedup.block_kv_name.data(), 0) != 0)
	{
		int fd1 = open(dedup.block_kv_name.data(), O_CREAT);
		close(fd1);
	}
	dedup.file_kv_name = dir + string("/filekv.md");
	if (access(dedup.file_kv_name.data(), 0) != 0)
	{
		int fd2 = open(dedup.file_kv_name.data(), O_CREAT);
		close(fd2);
	}
	dedup.block_kv.clear();
	dedup.file_kv.clear();
}

void store_meta()
{
	cout<<"Begin store meta..."<<endl;
	string filename;
	string line_cont;
	int count = 0;
	// store the file kv
	filename = dedup.file_kv_name;
	ofstream file_os(filename.data());
	if (!file_os)
	{
		cout<<filename<<" cannot open"<<endl;
		return;
	}
	map<string, string>::iterator file_iter = dedup.file_kv.begin();
	while (file_iter != dedup.file_kv.end())
	{
		line_cont = file_iter->first + "|" + file_iter->second;
		file_os<<line_cont<<endl;
		line_cont = "";
		count++;
		file_iter++;
	}
	/*
	for (file_iter = dedup.file_kv.begin(); file_iter < dedup.file_kv.end(); file_iter++)
	{
		line_cont = file_iter->first + "|" + file_iter->second;
		file_os<<line_cont<<endl;
		line_cont = "";
		count++;
	}
	*/
	file_os.close();
	cout<<"file KV items = "<<count<<endl;
	count = 0;

	// store the block kv
	filename = dedup.block_kv_name;
	ofstream block_os(filename);
	if (!block_os)
	{
		cout<<filename<<" cannot open"<<endl;
		return;
	}
	map<string, Block_Mgt>::iterator block_iter = dedup.block_kv.begin();
	while (block_iter != dedup.block_kv.end())
	{
		line_cont = block_iter->first + "|" + to_string(block_iter->second.id) + "|" + to_string(block_iter->second.ref);
		block_os<<line_cont<<endl;
		line_cont = "";
		count++;
		block_iter++;
	}
	/*
	for (block_iter = dedup.block_kv.begin(); block_iter < dedup.block_kv.end(); block_iter++)
	{
		line_cont = block_iter->first + "|" + to_string(block_iter->second.id) + "|" + to_string(block_iter->second.ref);
		block_os<<line_cont<<endl;
		line_cont = "";
		count++;
	}
	*/
	block_os.close();
	cout<<"block KV items = "<<count<<endl;
}

void load_meta()
{
	string filename;
	string line_cont;
	vector<string> sub_str;
	int count = 0;
	// load the file kv
	ifstream file_is;
	filename = dedup.file_kv_name;
	file_is.open(filename.data());
	while(getline(file_is, line_cont))
	{
		split_str(line_cont, "|", sub_str);
		dedup.file_kv.insert(pair<string, string>(sub_str[0], sub_str[1]));
		sub_str.clear();
		line_cont = "";
		count++;
	}
	file_is.close();
	cout<<"load file KV items = "<<count<<endl;
	count = 0;

	// load the block kv
	ifstream block_is;
	filename = dedup.block_kv_name;
	block_is.open(filename.data());
	while(getline(block_is, line_cont))
	{
		Block_Mgt blkmgt;
		split_str(line_cont, "|", sub_str);
		blkmgt.checksum = sub_str[0];
		blkmgt.id = stoul(sub_str[1]);
		blkmgt.ref = stoul(sub_str[2]);
		if (blkmgt.id > dedup.block_uuid)
			dedup.block_uuid = blkmgt.id;
		dedup.block_kv.insert(pair<string, Block_Mgt>(sub_str[0], blkmgt));
		sub_str.clear();
		line_cont = "";
		count++;
	}
	block_is.close();
	cout<<"load block KV items = "<<count<<endl;
}

void split_str(const string& src, const string& separator, vector<string>& dest)
{
	string str = src;
	string substring;
	string::size_type start = 0, index;
	dest.clear();
	index = str.find_first_of(separator, start);
	do
	{
		if(index != string::npos)
		{
			substring = str.substr(start, index - start);
			dest.push_back(substring);
			start = index + separator.size();
			index = str.find(separator, start);
			if (start == string::npos) break;
		}
	} while(index != string::npos);

	substring = str.substr(start);
	dest.push_back(substring);
}

int add(const string filename)
{
	cout<<"add file : "<<filename<<endl;
	// check the file exist
	if (dedup.file_kv.find(filename) != dedup.file_kv.end())
	{
		return FILE_EXIST;
	}

	// read the content;
	char *cont;
	FILE *f = fopen(filename.data(), "r");
	fseek(f, 0, SEEK_END);
	long length = ftell(f);
	cont = (char *)malloc(length + 1);
	rewind(f);
	fread(cont, sizeof(char), length, f);
	cont[length] = '\0';
	
	// split the file
	char buffer[BLOCK_SIZE];
	//unsigned char md5_checksum[32 + 1] = {0};
	string md5_checksum;
	int index = 0;
	string newfile;
	int block_size = 0;
	vector<uint32_t> ids;
	while(length > 0)
	{
		memset(buffer, '\0', BLOCK_SIZE);
		md5_checksum = "";
		//memset(md5_checksum, '\0', 33);
		if (length > BLOCK_SIZE)
			block_size = BLOCK_SIZE;
		else
			block_size = (int)length;    // the last part
		memcpy(buffer, &cont[index], BLOCK_SIZE);
		md5_checksum = md5(buffer, block_size);
		//md5(buffer, BLOCK_SIZE, md5_checksum);
		//md5_2_str(md5_checksum);
		map<string, Block_Mgt>::iterator iter = dedup.block_kv.find(md5_checksum);
		if(iter == dedup.block_kv.end())
		{    // new block
			Block_Mgt block;
			block.id = dedup.block_uuid;
			block.ref = 1;
			block.checksum = md5_checksum;
			dedup.block_kv.insert(pair<string, Block_Mgt>(md5_checksum, block));
			ids.push_back(block.id);
			newfile = to_string(block.id) + string(FILE_SUBFIX);
			write_to_disk(newfile.data(), buffer, block_size);
			dedup.block_uuid++;
		} else {
			// increase the ref
			iter->second.ref++;
			ids.push_back(iter->second.id);
		}
		length -= BLOCK_SIZE;
		index += block_size;
	}
	string str_ids;
	str_ids = "";
	uint32_t i;
	for(i = 0; i < ids.size() - 1; i++)
	{
		str_ids += to_string(ids[i]) + IDS_DELIMIT;
	}
	str_ids += to_string(ids[i]);
	dedup.file_kv.insert(pair<string, string>(filename, str_ids));
	free(cont);
	cont = NULL;
	return 0;
}

int remove(const string filename)
{
	cout<<"remove file : "<< filename<<endl;
	map<string, string>::iterator iter;
	iter = dedup.file_kv.find(filename);
	// check the file exist
	if (iter == dedup.file_kv.end())
	{
		return FILE_NOT_EXIST;
	}
	string str_ids;
	vector<string> ids;
	str_ids = iter->second;
	// remove the file in file_kv
	dedup.file_kv.erase(iter);

	split_str(str_ids, IDS_DELIMIT, ids);
	map<string, Block_Mgt>::iterator _iter = dedup.block_kv.begin();
	// reduce the ref
	while(_iter != dedup.block_kv.end())
	{
		for (uint32_t i = 0; i < ids.size(); i++)
		{
			if (_iter->second.id == stoul(ids[i]))
			{
				_iter->second.ref--;
			}
		}
		_iter++;
	}
	return 0;
}

int show(const string filename)
{
	cout<<"show content of file : "<<filename<<endl;
	map<string, string>::iterator iter;
	iter = dedup.file_kv.find(filename);
	// check the file exist
	if (iter == dedup.file_kv.end())
	{
		return FILE_NOT_EXIST;
	}
	string str_ids;
	vector<string> ids;
	str_ids = iter->second;
	split_str(str_ids, IDS_DELIMIT, ids);
	char *cont = NULL;
	cont = (char *)calloc(1, str_ids.size()*BLOCK_SIZE);
	char buffer[BLOCK_SIZE];
	string dst_file;
	int read_size;
	int pos = 0;
	for (uint32_t i = 0; i < ids.size(); i++)
	{
		memset(buffer, '\0', BLOCK_SIZE);
		dst_file = ids[i] + string(FILE_SUBFIX);
		read_size = read_from_disk(dst_file.data(), buffer);
		memcpy(&cont[pos], buffer, read_size);
		pos += read_size;
	}
	cout<<"file : "<<filename<<" cont is : "<<cont<<endl;
	free(cont);
	return 0;
}

int list()
{
	cout<<"list all file"<<endl;
	map<string, string>::iterator iter;
	iter = dedup.file_kv.begin();
	while(iter != dedup.file_kv.end())
	{
		cout <<"file : "<< iter->first <<" include block : "<<iter->second<<endl;
		iter++;
	}
	return 0;
}

void usage()
{
	cout<<"usage :  ./dedup -a filename"<<endl;
}

int main(int argc, char *agrv[])
{
	int ret = 0;
	if (argc < 2)
	{
		usage();
		return -1;
	}
	string dedup_op = agrv[1];
	string filename;
	if (argc > 2)
		filename = agrv[2];

	init();
	load_meta();
	if (strcmp(dedup_op.data(), "-a") == 0)
	{
		// add
		ret = add(filename);
		cout<<"add ret = "<<ret<<endl;
	} else if (strcmp(dedup_op.data(), "-r") == 0)
	{
		// remove
		ret = remove(filename);
		cout<<"remove ret = "<<ret<<endl;
	} else if (strcmp(dedup_op.data(), "-s") == 0)
	{
		// show content
		ret = show(filename);
		cout<<"show ret = "<<ret<<endl;
	} else if (strcmp(dedup_op.data(), "-l") == 0)
	{
		// list
		ret = list();
		cout<<"list ret = "<<ret<<endl;
	} else if (strcmp(dedup_op.data(), "-h") == 0)
	{
		// help
		usage();
	}
	store_meta();
	return 0;
}