#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <iostream>
#include <stdio.h>
#include <string>
#include <vector>
#include <map>
#include "BigQ.h"

typedef enum {heap, sorted, tree} fType;
static const char *enum_arr[] = {"heap","sorted","tree"};

// stub DBFile header..replace it with your own DBFile.h 

typedef struct MetaData {
	//public:
	char filename[20];
	int type;
	OrderMaker  o;
	int runlen;
};

class GenericDBFileImplementation {
public:
	virtual void Create (char *filename,void * startup) = 0;
	virtual void Open (char *filename,OrderMaker * o,int len) = 0;
	virtual void Load (Schema &myschema, char *loadpath) = 0;
	virtual void MoveFirst () = 0;
	virtual void MoveLast () = 0;
	virtual void Add (Record *addme) = 0;
	virtual void WriteToFile() = 0;
	virtual void FinishJob() = 0;
	virtual int GetNext (Record &fetchme) = 0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) =0;
};

class SortImplementation: public GenericDBFileImplementation {
private:
	int cnt;
	//static SortImplementation* implementation;
	char * fileName;
	File file;
	Page page;
	File buffFile;
	std::vector<RunPage> pageVector;
	RunPage *runpage;
	
	OrderMaker *order;
	OrderMaker QueryOrder;
	OrderMaker LiteralOrder;
	bool valid_QueryOrder;
    	bool QueryOrderGenerated;
	int runlen;
	
	//SortImplementation(SortImplementation const&){};
	//SortImplementation& operator=(SortImplementation const&){};
public:
	SortImplementation() {};
	//static SortImplementation* GetInstance();
	void Create(char *filename,void * startup) {
		typedef struct util{OrderMaker *o; int l;Schema *mySchema;};
		fileName = filename;
		file.Open(0,fileName);
		//printf("\n-------------------------\nInside Create");
		buffFile.Open(0,"buffFile.bin");
		order = ((util *)startup)->o;
		runlen = ((util *)startup)->l;
		//order->Print();
		runpage = new RunPage(runlen,&buffFile,order);
		valid_QueryOrder = false;
    		QueryOrderGenerated = false;
		cnt=0;
	}
	void Open(char *filename, OrderMaker *o, int len){
		fileName = filename;
		file.Open(1,filename);
		runlen = len;
		order = o;
		//printf("\n-------------------------\nInside Open");
		//order->Print();
		buffFile.Open(0,"buffFile.bin");
		valid_QueryOrder = false;
    		QueryOrderGenerated = false;
		runpage = new RunPage(runlen,&buffFile,order);
	}
	void Load (Schema &myschema, char *loadpath);
	void MoveFirst ();
	void Add (Record *addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	void FinishJob();
	void AddRec (Record *rec);
	void RefreshFile() {
		file.Close();
		if(fileName != NULL)
			file.Open(0,fileName);
	};
	void Close() {
		file.Close();
		//delete runpage;
	};
	void MoveLast() {
		page.EmptyItOut ();
		if(file.GetLength() == 0) {
			page.SetPageLoc(0);
		} else {
			page.SetPageLoc(file.GetLength()-1);
		}
	};
	void WriteToFile() {
		file.AddPage(&page,page.GetPageLoc());
	};
	int GetNextwithCNF(Record &fetchme, CNF &cnf, Record &literal);
	int BinarySearch (Record &fetchme, Record &literal, File * searchFile, int start, int end);
};

class HeapImplementation: public GenericDBFileImplementation {
private:
	//static HeapImplementation* implementation;
	File file;
	Page page;
	
	//HeapImplementation(HeapImplementation const&){};
	//HeapImplementation& operator=(HeapImplementation const&){};
public:
	HeapImplementation() {};
	//static HeapImplementation* GetInstance();
	void Create(char *filename,void * startup) {
		file.Open(0,filename);
	}
	void Open(char *filename,OrderMaker * o,int len){
		file.Open(1,filename);
	}
	void Load (Schema &myschema, char *loadpath);
	void MoveFirst ();
	void Add (Record *addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	void FinishJob(){
		page.EmptyItOut();
		file.Close();
	};
	void MoveLast() {
		page.EmptyItOut ();
		if(file.GetLength() == 0) {
			page.SetPageLoc(0);
		} else {
			page.SetPageLoc(file.GetLength()-1);
		}
	}
	
	void WriteToFile() {
		file.AddPage(&page,page.GetPageLoc());
	}
};

class DBFile {
private:
	int metaFilePntr;
	std::map<string, MetaData> metalist;
	
	//File file;
	//Page page;
	string filename;
	GenericDBFileImplementation *implementation;
	
public:
	DBFile (); 
	void WriteMetaFileList();
	string getFileName();
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};


#endif
