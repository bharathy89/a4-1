#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include "Pipe.h"
#include <stdio.h>
#include <string.h>

using namespace std;

extern struct AndList *final;
ComparisonEngine DBComp;

// stub file .. replace it with your own DBFile.cc

typedef struct ThreadParameters {
	Pipe *pipe;
	GenericDBFileImplementation *implementation;
	Schema *mySchema;
};

//SortImplementation * SortImplementation::implementation=NULL;
//HeapImplementation * HeapImplementation::implementation=NULL;

void* producer(void * arg);
void* consumer(void * arg);

DBFile::DBFile () {
	metaFilePntr = open(".header",O_RDWR,S_IRUSR | S_IWUSR);
	lseek(metaFilePntr,0,SEEK_SET);
	int size=0,i; 
	
	if(!read(metaFilePntr,&size,sizeof(int))) {
		size=0;
			
	}
	//printf("\nsize : %d",size);
	for(i=0;i<size;i++) {
		MetaData *buff = (MetaData *)malloc(sizeof(MetaData));
		read(metaFilePntr,buff,sizeof(MetaData));
		string str = buff->filename;
		metalist.insert(pair<string,MetaData>(str,*buff));
		//cout << str;
	}
	close(metaFilePntr);
}

void DBFile::WriteMetaFileList() {
	metaFilePntr = open(".header", O_TRUNC | O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	lseek(metaFilePntr,0,SEEK_SET);
	int size = metalist.size();
	int i;
	write(metaFilePntr,&size,sizeof(int));
	std::map<string, MetaData>::iterator iter;

	for (iter=metalist.begin();iter!=metalist.end();iter++){
		MetaData &meta= iter->second; 		
		write(metaFilePntr, &meta, sizeof(MetaData));
	}
	close(metaFilePntr);
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
	char space[200];
	typedef struct util{OrderMaker *o; int l;Schema *mySchema;};
	sprintf(space,"%s %c\n",f_path,f_type+'0');
	MetaData buff;
	strcpy(buff.filename, f_path);
	buff.type = f_type;
	
	string str = buff.filename;
	filename = str;
	if(f_type == 0) {
		implementation = new HeapImplementation();
	} else if(f_type == 1) {
		//printf("under construction");
		implementation = new SortImplementation();		
		buff.o=*((util *)startup)->o;
		//buff.o.Print();
	}
	metalist[str]=buff;
	implementation->Create(f_path,startup);
	WriteMetaFileList();
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	implementation->Load(f_schema,loadpath);
}

string DBFile::getFileName() {
	return filename;
}
int DBFile::Open (char *f_path) {	
	string str = f_path;
	if(metalist.find(str) != metalist.end()) {
		MetaData buff = metalist[str];
		cout << "File found !!";
		filename = f_path;
		if(buff.type == 0) {
			implementation = new HeapImplementation();
		} else if(buff.type == 1) {
			implementation = new SortImplementation();
			//printf("\n==========================================\n");
			//buff.o.Print();	
			//printf("\n==========================================\n");
		}
		implementation->Open(f_path,&(metalist[str]).o,(metalist[str]).runlen);
		WriteMetaFileList();
		return 1;
	}	 
	cout << "File not found !!";
	WriteMetaFileList();
	return 0;
}

void DBFile::MoveFirst () {
	implementation->MoveFirst();
}

int DBFile::Close () {
	implementation->FinishJob();
	
}

void DBFile::Add (Record &rec) {
	implementation->Add(&rec);
}

int DBFile::GetNext (Record &fetchme ) {
	return implementation->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	return implementation->GetNext(fetchme,cnf,literal);
}

/*HeapImplementation* HeapImplementation::GetInstance() {
	if(!implementation)
		implementation = new HeapImplementation();
	return implementation;	
}*/

void HeapImplementation::Load (Schema &f_schema, char *loadpath) {
	Record temp;
	int counter = 0;
	ComparisonEngine comp;
	FILE *tableFile = fopen (loadpath, "r");
	page.EmptyItOut();
    while (temp.SuckNextRecord (&f_schema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}
		//temp.Print(&f_schema);
		// page is appended to the end of file always in this method.
		if(page.Append(&temp)==0) {	
			if(file.GetLength() == 0) {
				page.SetPageLoc(0);
			} else {
				page.SetPageLoc(file.GetLength()-1);
			}
			
			file.AddPage(&page,page.GetPageLoc());
			page.EmptyItOut ();
			page.Append(&temp);
			
		} 
	}
	cout << "loaded " << counter << "recs\n";

	if(file.GetLength() == 0) {
		page.SetPageLoc(0);
	} else {
		page.SetPageLoc(file.GetLength()-1);
	}
	file.AddPage(&page,page.GetPageLoc());
	page.EmptyItOut ();
}

void HeapImplementation::Add (Record *rec) {
	if(page.Append(rec)==0) {
		file.AddPage(&page,page.GetPageLoc());
		page.EmptyItOut ();
		if(file.GetLength() == 0) {
			page.SetPageLoc(0);
		} else {
			page.SetPageLoc(file.GetLength()-1);
		}
		page.Append(rec);
	}
}

void HeapImplementation::MoveFirst () {
	page.EmptyItOut ();
	if(file.GetLength()==0) {
		printf("\nFile empty");
		return;
	}
	file.GetPage(&page,0);
	
}


int HeapImplementation::GetNext (Record &fetchme ) {
	if(page.GetFirst(&fetchme)==0) {
		off_t pageLoc = page.GetPageLoc();
		page.EmptyItOut();
		if(pageLoc+2 >= file.GetLength()) {
			return 0;
		}
		
		file.GetPage(&page,pageLoc+1);
		page.GetFirst(&fetchme);
	}
	return 1;
}


int HeapImplementation::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
	while(1) {
		if(page.GetFirst(&fetchme)==0) {
			off_t pageLoc = page.GetPageLoc();
			page.EmptyItOut();
			if(pageLoc+2 >= file.GetLength()) {
				return 0;
			}
			file.GetPage(&page,pageLoc+1);
			page.GetFirst(&fetchme);
		}
		if (comp.Compare (&fetchme, &literal, &cnf)) {
			return 1;
		}
	}
}

/*SortImplementation* SortImplementation::GetInstance() {
	if(!implementation)
		implementation = new SortImplementation();
	return implementation;	
}*/

void SortImplementation::AddRec (Record *rec) {	
	if(page.Append(rec)==0) {
		file.AddPage(&page,page.GetPageLoc());
		page.EmptyItOut ();
		if(file.GetLength() == 0) {
			page.SetPageLoc(0);
		} else {
			page.SetPageLoc(file.GetLength()-1);
		}
		page.Append(rec);
	}
}

void SortImplementation::Load (Schema &f_schema, char *loadpath) {
	/*Record temp;
	int counter = 0;
	ComparisonEngine comp;
	FILE *tableFile = fopen (loadpath, "r");
	page.EmptyItOut();
    while (temp.SuckNextRecord (&f_schema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}
		temp.Print(&f_schema);
		// page is appended to the end of file always in this method.
		if(page.Append(&temp)==0) {	
			if(file.GetLength() == 0) {
				page.SetPageLoc(0);
			} else {
				page.SetPageLoc(file.GetLength()-1);
			}
			
			file.AddPage(&page,page.GetPageLoc());
			page.EmptyItOut ();
			page.Append(&temp);
			
		} 
	}
	cout << "loaded " << counter << "recs\n";

	if(file.GetLength() == 0) {
		page.SetPageLoc(0);
	} else {
		page.SetPageLoc(file.GetLength()-1);
	}
	file.AddPage(&page,page.GetPageLoc());
	page.EmptyItOut ();*/
}


void SortImplementation::Add (Record *rec) {
	cnt++;
	
	if(runpage->Append(rec) == 0) {
		runpage->WriteOut();
		pageVector.push_back(*runpage);
		runpage = new RunPage(runlen,&buffFile,order);			
		runpage->Append(rec);	
	}
}

void SortImplementation::FinishJob() {
	if(cnt==0) {
		Close();		
		return;
        }
	printf("\n---------\nCount : %d\n",cnt);
	cnt=0;
	runpage->WriteOut();
	pageVector.push_back(*runpage);

	int buffsz = 100;
	Pipe input (buffsz);
	Pipe output (buffsz);

	ThreadParameters producerParam = {&input,this,NULL};
	pthread_t thread1;
	pthread_create (&thread1, NULL, producer, (void *)&producerParam);
	pthread_t thread2;
	ThreadParameters consumerParam = {&output,this,NULL};
	pthread_create (&thread2, NULL, consumer, (void *)&consumerParam);
	Params param = {NULL, &pageVector};
	BigQ bq (input, output, *order, runlen, &param);
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	pageVector.clear();
	free(runpage);
	Close();
}

void *producer (void *arg) {

	Pipe *myPipe = ((ThreadParameters *) arg)->pipe;
	SortImplementation *implementation = (SortImplementation *)((ThreadParameters *) arg)->implementation;
	Record temp;
	int counter = 0;
	//printf("Inside producer");	
	
	implementation->MoveFirst ();

	while (implementation->GetNext (temp) == 1) {
		counter += 1;
		if (counter%100000 == 0) {
			 cerr << " producer: " << counter << endl;	
		}
		myPipe->Insert (&temp);
	}

	implementation->Close();
	myPipe->ShutDown ();

	cout << "\n producer: inserted " << counter << " recs into the pipe\n";
}

void *consumer (void *arg) {
	
	Pipe *myPipe = ((ThreadParameters *) arg)->pipe;
	SortImplementation *implementation = (SortImplementation *)((ThreadParameters *) arg)->implementation;
	ComparisonEngine ceng;
	
	int err = 0;
	int i = 0;
	bool first = true;
	Record rec;
	//printf("inside consumer");
	while (myPipe->Remove (&rec)) {
		if(first) {
			implementation->RefreshFile();
			implementation->MoveLast();			
			first = false;
		}
		implementation->AddRec(&rec);
		i++;
	}
	implementation->WriteToFile();
	implementation->Close();
	cout << "\n consumer: removed " << i << " recs from the pipe\n";			
}

void SortImplementation::MoveFirst () {
	page.EmptyItOut ();
	if(file.GetLength()==0) {
		printf("\nFile empty");
		return;
	}
	file.GetPage(&page,0);
}


int SortImplementation::GetNext (Record &fetchme ) {
	if(page.GetFirst(&fetchme)==0) {
		off_t pageLoc = page.GetPageLoc();
		page.EmptyItOut();
		if(pageLoc+2 >= file.GetLength()) {
			return 0;
		}
		
		file.GetPage(&page,pageLoc+1);
		page.GetFirst(&fetchme);
	}
	return 1;
}


int SortImplementation::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	if (QueryOrderGenerated) {
        	if ( ! GetNext(fetchme)) {
        		return 0;
        	}
        
        	return GetNextwithCNF(fetchme, cnf, literal);  
    	}
    
   	int res = cnf.BuildQueryOrder(QueryOrder, *order, LiteralOrder);
    
    	if (res) {
        	QueryOrderGenerated     = true;
        	valid_QueryOrder        = true;
        
        	if (BinarySearch(fetchme, literal, &file, 0, file.GetLength()-2)) {
            		return GetNextwithCNF(fetchme, cnf, literal);
            
        	} else {
            		return 0;
        	}
    	} else {
        	QueryOrderGenerated     = true;
        	valid_QueryOrder        = false;
        	MoveFirst();
        	GetNext(fetchme);
        	return GetNextwithCNF(fetchme, cnf, literal);
    	}
	return 0;
}


int SortImplementation :: GetNextwithCNF(Record &fetchme, CNF &cnf, Record &literal)
{
    	while (!(DBComp.Compare(&fetchme, &literal, &cnf)))
    	{
        	if ( ! GetNext(fetchme)) {
            		return 0;
        	}
        
        	if (valid_QueryOrder ) {
            		if (DBComp.Compare (&fetchme, &QueryOrder, &literal, &LiteralOrder)) {
                		return 0;
            		}
        	}
    	}
    	return 1;
}



int SortImplementation :: BinarySearch (Record &fetchme, Record &literal, File * searchFile, int start, int end) {
	int pageNumber =0;
    	bool first_match = false;
    	while (start<end) 
    	{
        	pageNumber = (start + end)/2;
        
        	if (pageNumber == start) {
            		if (first_match) {
                		break;
            		}
            		pageNumber++;
        	}
        
        	page.EmptyItOut();
        	searchFile->GetPage(&page, pageNumber);
        	GetNext(fetchme);
        
        	if (DBComp.Compare (&fetchme, &QueryOrder, &literal, &LiteralOrder) == 0) {
            		end = pageNumber;
            		first_match = true;
        	} else if (DBComp.Compare (&fetchme, &QueryOrder, &literal, &LiteralOrder) < 0) {
            		start = pageNumber;
        	} else if (DBComp.Compare (&fetchme, &QueryOrder, &literal, &LiteralOrder) > 0) {
            		end = pageNumber-1;
        	}	
        
    	}
    	pageNumber = start;
    	page.EmptyItOut();
    	searchFile->GetPage(&page, pageNumber);
    	GetNext(fetchme); 
    
    	while (DBComp.Compare (&fetchme, &QueryOrder, &literal, &LiteralOrder)) {
        	if ( ! (page.GetFirst(&fetchme)) ) {
            		if (first_match) {
                		pageNumber++;
                
                		if (pageNumber < (searchFile->GetLength()-1) ) {
                    
                    			searchFile->GetPage(&page, pageNumber);
                    			GetNext(fetchme);
                		} else {
                    			return 0;
                		}
            		}
            		return 0;
        	}
    	}
   	return 1;
}
