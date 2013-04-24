#include "BigQ.h"
#include "ComparisonEngine.h"
#include "Comparison.h"
#include <vector>
#include <algorithm>
#include <stdlib.h>
#include <string.h>
#include <typeinfo>

using namespace std;

void * tpmms(void *);

//Schema *mySchema;

//OrderMaker *om;

class Record;

class SortRecords {
private:
	OrderMaker* orderMaker;
	ComparisonEngine comparer;
	
public:
	
	SortRecords(OrderMaker* sortOrder):orderMaker(sortOrder) {}
	
	bool operator()(Record *r1, Record *r2) {
		//r1->Print(mySchema);
		//r2->Print(mySchema);
		int result = comparer.Compare(r1, r2, orderMaker);
		if(result < 1) {
			return true;
		} else {
			return false;
		}
	}

	bool operator() (HeapElem *r1, HeapElem *r2) {
		int result = comparer.Compare((Record *)(r1->rec), (Record *)(r2->rec), orderMaker); 
		if(result < 1) {
			return false;
		} else {
			return true;
		}
	}
	
};


void insertSort(vector<Record *> arr,OrderMaker *om){
    int maxIdx;    
	int a;
	ComparisonEngine comparer;
	Record *temp = new Record();
    for(unsigned int i = 0; i<arr.size()-1;i++){      
		for(int j = i+1; j<arr.size();j++){
			int res = comparer.Compare(arr[i], arr[j],om);           	
			
			if(res > 0){
            	temp->Consume(arr[i]);
				arr[i]->Consume(arr[j]);
				arr[j]->Consume(temp);
           	}    
        }	
    } 
	delete temp;   
}


RunPage :: ~RunPage () {
}

RunPage :: RunPage (int runlength,File *bFile,OrderMaker *o) {
	pageSize = runlength;
	curSizeInBytes = sizeof (int);
	numRecs = 0;
	order = o;
	file = bFile;
}


int RunPage :: Append (Record *addMe) {
	char *b = addMe->GetBits();
	//printf("\ncurSizeInBytes %d",curSizeInBytes); 
	if (curSizeInBytes + ((int *) b)[0] > PAGE_SIZE) {
		curSizeInBytes = sizeof(int) + ((int *) b)[0];
		pageSize--;
		if(pageSize==0) {
			curSizeInBytes = sizeof(int);
			return 0;
		}
	}
	Record* tempRecord = new Record();

	tempRecord->Consume(addMe);
	//tempRecord->Print(mySchema);	
	recList.push_back(tempRecord);
	curSizeInBytes += ((int *) b)[0];
	numRecs++;
	return 1;	
}

void RunPage:: Sort() {
	//std::sort(recList.begin(),recList.end(),compare);
	insertSort(recList,order);
}

void RunPage::GeneratePageAndAddtoFile() {
	typedef vector<Record *> RecordList;
	RecordList::const_iterator ci = recList.begin();
	Page *page = new Page();	
	
	while (ci != recList.end()) {
		//(*ci)->Print(mySchema);
		if(page->Append((*ci)) == 0) {
			if(file->GetLength() == 0) {
				page->SetPageLoc(0);
			} else {
				page->SetPageLoc(file->GetLength()-1);
			}
			
			file->AddPage(page,page->GetPageLoc());
			myPagesIndex.push_back(page->GetPageLoc());
			page->EmptyItOut();			
			page->Append((*ci));	
		}
		delete (*ci);
		ci++;
	}
	if(file->GetLength() == 0) {
		page->SetPageLoc(0);
	} else {
		page->SetPageLoc(file->GetLength()-1);
	}		
	file->AddPage(page,page->GetPageLoc());	
	myPagesIndex.push_back(page->GetPageLoc());
	//printf("size of pages Index : %d",myPagesIndex.size());
	page->EmptyItOut();
	recList.clear();
	delete page;
}

void RunPage::WriteOut() {
	Sort();
	GeneratePageAndAddtoFile();
}

int RunPage::GetFirstPage(Page *page) {
	if(myPagesIndex.size() > 0) {
		int pageIndex = myPagesIndex.front();
		myPagesIndex.pop_front();
		//printf("updated size of pagesIndexList : %d",myPagesIndex.size());
		file->GetPage(page,pageIndex);
		return 1;
	} 
	return 0;
}

int RunPage:: NumOfPages() {
	return myPagesIndex.size();
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen, void *parameters) {
	printf("Inside BigQ");
	// read data from in pipe sort them into runlen pages
	std::vector<RunPage> *pageList = new vector<RunPage>();	
	if(parameters) {	
		pageList = ((Params *)parameters)->runpageList;
	}
	param = new WorkerParameters(sortorder,20,in,out,NULL);	
	pthread_create (&bigQWorker, NULL, tpmms, (void *)param);	
	//pthread_join (bigQWorker,NULL);
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen, char *file, Schema* schema) {
	printf("Inside BigQ");
	// read data from in pipe sort them into runlen pages	
	//mySchema = schema;
	sortorder.Print();
	//om = &sortorder;
	param = new WorkerParameters(sortorder,20,in,out,(char *)strdup(file));		
	pthread_create (&bigQWorker, NULL, tpmms, (void *)param);	
}

void BigQ :: WaitUntilDone() {
	pthread_join (bigQWorker,NULL);
}

BigQ::~BigQ () {
}

void *tpmms(void * in) {
	printf("\nNow running Tpmms..");	
	File file;
	WorkerParameters *param = (WorkerParameters *)in;
	char *buffname = "buff.bin";
		
	if(param->filename) {
		char * fname = (char *)malloc(sizeof(param->filename)+sizeof(buffname));
		strcpy(fname,param->filename);
		strcat(fname,buffname);
		buffname = fname; 
	}
	cout << "\nbuff file name : "<<buffname<<endl;
	file.Open(0,buffname);
	Record *rec;
	rec = new Record();
	int cntr=0;
	RunPage *runpage;
	param->sortorder->Print();
	printf("\nrunlen %d",param->runlen);
	runpage = new RunPage(param->runlen,&file,param->sortorder);
	//printf("\nsize : %d\n",param->pageList->size());
	std::vector<RunPage> pageVector;
	printf("\n before page vector size:  %d \n",pageVector.size());
	while (param->pipe->Remove (rec)) {
		//printf("accepting rec");
		cntr++;
		//rec->Print(mySchema);
		if(cntr%10000==0)
			printf("\n%d",cntr);
		if(runpage->Append(rec) == 0) {
			
			runpage->WriteOut();
			pageVector.push_back(*runpage);
			runpage = new RunPage(param->runlen,&file,param->sortorder);	
			runpage->Append(rec);			
		}
	}
	runpage->WriteOut();
	pageVector.push_back(*runpage);
	printf("\n%s has no of input records : %d",buffname, cntr);
	
	delete runpage;

	Record record;
	//printf("\nvalue : %d",pageVector.size());
	std::vector<HeapElem *> recordArr; //(HeapElem *)malloc(pageVector.size()*PAGE_SIZE);
	int pageTracker[pageVector.size()];
	int index=0,heapIndex =0; 
	HeapElem *buff;
	Page *page = new Page();
	printf("\nNow second phase of tpmms starts");
	int count=0;
	printf("\n pageVector size : %d",pageVector.size());
	while(index < pageVector.size()) {	
		if(pageVector[index].GetFirstPage(page)) {
			//printf("No of pages : %d ",pageVector[index].NumOfPages());
			pageTracker[index] = page->GetNumRecords();
			//printf("\n number of records : %d",pageTracker[index]);
			count=0;
			while(page->GetFirst(&record)) {
				//record.Print(mySchema);					
				buff = new HeapElem(index,&record);	
				
				recordArr.push_back(buff);//[heapIndex++]= *buff;
				count++;
			}
			page->EmptyItOut();
		}
		index++;
	}
	printf("recordArr size : %d",recordArr.size());
	std::sort(recordArr.begin(),recordArr.end(),SortRecords(param->sortorder));
	//printf("index : %d",index);
	//MyHeap heap (recordArr, heapIndex);
	int cn=0;
	while(recordArr.size()!=0) {
		//	printf("heap size : %d",heap.Size());
		cn++;	
		if(cn%10000==0)
			printf("\n inserted %d",cn);	
		HeapElem elem = *(recordArr.back());
		//heap.RemoveTop();
		//elem.rec->Print(param->schema);
		param->outpipe->Insert(elem.rec);
		recordArr.pop_back();
		Record record;
		//printf("\nindex value : %d \n",elem.index);
		//page->EmptyItOut();
		pageTracker[elem.index]-=1;
		if(pageTracker[elem.index] == 0) {
			page->EmptyItOut();
			
			if(pageVector[elem.index].GetFirstPage(page)) {
				pageTracker[elem.index] = page->GetNumRecords();
				//printf("No of records per page : %d",page->GetNumRecords());
				while(page->GetFirst(&record)) {
					buff = new HeapElem(elem.index,&record);			
					recordArr.push_back(buff);					
					//heap.Insert(*buff);
				}
				std::sort(recordArr.begin(),recordArr.end(),SortRecords(param->sortorder));
				//heap.Heapify();
				page->EmptyItOut();			
			}
		}
	}
	printf("final count : %d",cn);		
	param->outpipe->ShutDown ();
	printf("going out of tpmms");
	if (remove(buffname) == -1) {
  		perror("Error in deleting a file");
	}
	//	cout<<"Page List Size : "<<pageVector.size()<<"\n";
}

