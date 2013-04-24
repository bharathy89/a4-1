#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "ComparisonEngine.h"
#include "Comparison.h"
#include <stdlib.h>
#include <list>
#include <vector>

using namespace std;

class Record;

class HeapElem {
	public:	
	int index;
	Record *rec;	
	HeapElem() {
	};
	HeapElem (int i, Record *r) {
		index = i;
		rec = new Record();
		rec->Consume(r);	
	};
	
	void Print(Schema *mySchema) {
		cout <<" index value : "<<index;
		rec->Print(mySchema);
	};
};


class RunPage {
private:
	std::vector<Record *> recList;
	std::list<int> myPagesIndex;
	OrderMaker *order;
	int pageSize;
	int numRecs;
	int curSizeInBytes;
	File *file;
public:
	
	RunPage (int runlength,File *bFile,OrderMaker *o);
	virtual ~RunPage ();

	// this appends the record to the end of a page.  The return value
	// is a one on success and a aero if there is no more space
	// note that the record is consumed so it will have no value after
	int Append (Record *addMe);

	void Sort();
	void GeneratePageAndAddtoFile();
	void WriteOut();
	int GetFirstPage(Page *page);
	int NumOfPages();
};

class WorkerParameters {
public:
	OrderMaker *sortorder;
	int runlen;
	Pipe *pipe;
	Pipe *outpipe;
	std::vector<RunPage> *pageList;
	char *filename;
	WorkerParameters(OrderMaker &_sortorder,int run, Pipe &_in, Pipe &_out,char *file) {
		sortorder = &_sortorder;
		runlen = run;
		pipe = &_in;
		outpipe = &_out;
		pageList = NULL;
		filename = file;
	};
};

typedef struct {Schema *mySchema; std::vector<RunPage> *runpageList;} Params;

class BigQ {
private:
	pthread_t bigQWorker;
	WorkerParameters *param;
public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen, void *param);
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen, char * filename,Schema *schema);
	~BigQ ();
	void WaitUntilDone();
};

#endif
