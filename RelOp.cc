#include "RelOp.h"

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) 
{
    SFA = new SelectFileArgs(inFile, outPipe, selOp, literal);
	printf("started thread");
    pthread_create (&thread, NULL, SelectFileWorker, (void *)SFA);    
}

void SelectFile::WaitUntilDone () 
{
        pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) 
{
    
}

void * SelectFileWorker (void * SFileArgs)
{
    SelectFileArgs * SFA = (SelectFileArgs *) SFileArgs;
    int count=0;
    Record buffer;
    string str = SFA->inFile->getFileName();
    while (SFA->inFile->GetNext (buffer, *SFA->selOp, *SFA->literal))
    {	
		
        SFA->outPipe->Insert(&buffer);
		count++;
    }
	printf("\ncount : %d",count);
    SFA->outPipe->ShutDown ();
    cout<<str<<" exited!!";
}

void Project :: Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput, Schema *schema) 
{ 
    SFA = new SelectFileArgs(inPipe, outPipe, keepMe, numAttsInput, numAttsOutput);
    pthread_create (&thread, NULL, ProjectWorker, (void *)SFA);
}

void Project :: WaitUntilDone () 
{ 
    pthread_join (thread, NULL);
}

void Project :: Use_n_Pages (int n) 
{ 
    
}

void * ProjectWorker (void * SFileArgs)
{
    SelectFileArgs * SFA = (SelectFileArgs *) SFileArgs;
    
    Record buffer;
    int cnt=0;
    while (SFA->inPipe->Remove (&buffer))
    {
		cnt++;
        buffer.Project(SFA->keepMe, SFA->numAttsOutput, SFA->numAttsInput);
        SFA->outPipe->Insert(&buffer);
    }
    printf("\nno of records removed : %d",cnt);
    SFA->outPipe->ShutDown ();
    
    
}

void Sum :: Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) 
{ 
    SFA = new SelectFileArgs(inPipe, outPipe, computeMe);
    pthread_create (&thread, NULL, SumWorker, (void *)SFA);
    
}

void Sum :: WaitUntilDone () 
{ 
    pthread_join (thread, NULL);
}

void Sum :: Use_n_Pages (int n) 
{ 
    
}

void * SumWorker (void * SFileArgs)
{
    SelectFileArgs * SFA = (SelectFileArgs *) SFileArgs;
    
    Record buffer;
    
    double sum =0.0;
    
    while (SFA->inPipe->Remove (&buffer))
    {
        int intResult           = 0;
        double doubleResult     = 0.0;
        SFA->computeMe->Apply(buffer, intResult, doubleResult);
        sum += intResult + doubleResult;
        
    }
    
    Attribute DA = {"double", Double};
    Schema out_sch ("out_sch", 1, &DA);
    
    char double_string[100]; 
    
    sprintf(double_string, "%f|",sum);
    
    buffer.ComposeRecord(&out_sch, double_string);
    
    SFA->outPipe->Insert(&buffer);
    SFA->outPipe->ShutDown ();
}


void Join :: Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal, Schema *mySchema, Schema *mySchemaR)
{ 
	//OrderMaker left, right;
    SFA = new SelectFileArgs(inPipeL, inPipeR, outPipeL, outPipeR, outPipe, selOp, literal, mySchema, mySchemaR);
    int res = SFA->selOp->GetSortOrders(left, right);
	pthread_create (&thread, NULL, JoinWorker, (void *)SFA);
	//printf("\n\n %s",one);
	BigQ bqL (*SFA->inPipe, *SFA->outPipeL, left, 20, "one", mySchema);	
	BigQ bqR (*SFA->inPipeR, *SFA->outPipeR, right, 20, "two", mySchemaR);
	//bqL.WaitUntilDone();
	//bqR.WaitUntilDone();
}

void Join :: WaitUntilDone () 
{
    pthread_join (thread, NULL);   
}

void Join :: Use_n_Pages (int n) 
{ 
    
}

void * JoinWorker (void * SFileArgs)
{
		int cnt=0;
		Record bufferL, bufferR, *Result;
		SelectFileArgs * SFA = (SelectFileArgs *) SFileArgs;	
		OrderMaker left, right;    
    	int res = SFA->selOp->GetSortOrders(left, right);
	
		vector<Record *> RecordList_R;
        
        ComparisonEngine ceng;
        
        int *n =new int [SFA->mySchema->GetNumAtts() + SFA->mySchemaR->GetNumAtts()];
        
        for (int i=0; i<SFA->mySchema->GetNumAtts(); i++) 
        {
            n[i]=i;
        }
        for (int i=SFA->mySchema->GetNumAtts(), j=0; i<SFA->mySchema->GetNumAtts() + SFA->mySchemaR->GetNumAtts(); i++, j++) 
        {
            n[i]=j;
        }
        
        int Pipe_result = 1, count = 0, count1 =0, count2 = 0, supp_key =0;
        
        SFA->outPipeL->Remove(&bufferL);
        bufferL.Print(SFA->mySchema);
        SFA->outPipeR->Remove(&bufferR);
        bufferR.Print(SFA->mySchemaR);
        while (Pipe_result)
        {
			printf(".");
            RecordList_R.clear();
            //printf("here");
            if (ceng.Compare(&bufferL, &left, &bufferR, &right) < 0)
            {
                Pipe_result = SFA->outPipeL->Remove(&bufferL); 
            }
            else if (ceng.Compare(&bufferL, &left, &bufferR, &right) > 0)
            {
                Pipe_result =  SFA->outPipeR->Remove (&bufferR);
                //cout << "Count great " <<++count2 << endl;
            }
            else 
            {
                count = 0;
                //bufferL.Print(SFA->mySchema);
                while (ceng.Compare(&bufferL, &left, &bufferR, &right) == 0)
                {
                    //bufferR.Print(SFA->mySchemaR);
                    Record * tmp = new Record();
                    tmp->Copy(&bufferR);
                    RecordList_R.push_back(tmp);
                    
                    if (!SFA->outPipeR->Remove(&bufferR))
                    {
                        Pipe_result = 0;
                        break;
                    }
                    
                    count++;
                }
               
                count = 0;
                while (ceng.Compare(&bufferL, &left, RecordList_R[0], &right) == 0) 
                {
                    for (vector<Record *>::iterator it = RecordList_R.begin(); it <RecordList_R.end(); it++) 
                    {
                        Record *right_r = *it;
                        
                        Result = new Record();
                        Result->MergeRecords(&bufferL, right_r, SFA->mySchema->GetNumAtts(), SFA->mySchemaR->GetNumAtts(), n, SFA->mySchema->GetNumAtts() + SFA->mySchemaR->GetNumAtts(), SFA->mySchema->GetNumAtts());                        
						SFA->outPipe->Insert(Result);
                        ++count;   
                    }
                    
                    if (!SFA->outPipeL->Remove(&bufferL))
                    {
                        Pipe_result = 0;
                        break;
                    }              
                }      
            }           
        }
        while (SFA->outPipeL->Remove(&bufferL));
        while (SFA->outPipeR->Remove(&bufferR));
        delete [] n; 
        SFA->outPipe->ShutDown ();
}

void DuplicateRemoval :: Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) 
{ 
    SFA = new SelectFileArgs(inPipe, output, outPipe, mySchema);
    pthread_create (&thread, NULL, DuplicateRemovalWorker, (void *)SFA);
	sortorder = new OrderMaker(SFA->mySchema);
	sortorder->Print();
	BigQ bq (*SFA->inPipe, output, *sortorder, 20,"dup",SFA->mySchema);
	//bq.WaitUntilDone();
}

void DuplicateRemoval :: WaitUntilDone () 
{
    pthread_join (thread, NULL);
}

void DuplicateRemoval :: Use_n_Pages (int n) 
{ 
    
}

void * DuplicateRemovalWorker (void * SFileArgs)
{
    SelectFileArgs * SFA = (SelectFileArgs *) SFileArgs;
    
    OrderMaker sortorder(SFA->mySchema);
    ComparisonEngine ceng;
	Record rec;
    Record *last = NULL, *prev = NULL;	
	while (SFA->outPipeL->Remove(&rec)) {
		rec.Print(SFA->mySchema);
		prev = last;
        
        last = new Record();
        last->Copy(&rec);
        
        if (prev && last) 
        {
            if (ceng.Compare (prev, last, &sortorder) != 0) 
            {
                SFA->outPipe->Insert(prev);
            }
            
        }
        delete prev;
    }
	SFA->outPipe->Insert(last);
    
    SFA->outPipe->ShutDown();
    
}

void GroupBy :: Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) 
{
    SFA = new SelectFileArgs(inPipe, output, outPipe, groupAtts, computeMe);
	BigQ bq (*SFA->inPipe,*SFA->outPipeL, *SFA->groupAtts, 20,"grp",NULL);
	pthread_create (&thread, NULL, GroupByWorker, (void *)SFA);
}

void GroupBy :: WaitUntilDone () 
{ 
    pthread_join (thread, NULL);
}

void GroupBy :: Use_n_Pages (int n) 
{ 
    
}

void * GroupByWorker (void * SFileArgs)
{
    SelectFileArgs * SFA = (SelectFileArgs *) SFileArgs;
    
    Record buffer;
    
    ComparisonEngine ceng;
    
    Record rec;
    Record *last = NULL, *prev = NULL;
    
    double sum =0.0;
    
    Attribute DA = {"double", Double};
    Schema out_sch ("out_sch", 1, &DA);
    
    char double_string[1000]; 
    
    int count = 0;
    
    while (SFA->outPipeL->Remove (&rec)) 
    {
        prev = last;   
        last = new Record();
        last->Copy(&rec);
        if (prev && last) 
        {
            if (ceng.Compare (prev, last, SFA->groupAtts) != 0) 
            {
                sprintf(double_string, "%f|",sum); 
                buffer.ComposeRecord(&out_sch, double_string);
                
                int totAttsToKeep = SFA->groupAtts->numAtts + 1;
                int attsToKeep[totAttsToKeep];
                attsToKeep[0] = 0; 
                for(int i = 1; i < totAttsToKeep; i++)
                {
                    attsToKeep[i] = SFA->groupAtts->whichAtts[i-1];
                }
                Record final_rec;
				count++;
                final_rec.MergeRecords(&buffer, prev , 1, totAttsToKeep - 1, attsToKeep, totAttsToKeep, 1);
                
                SFA->outPipe->Insert(&final_rec);
                
                sum =0.0;
            }
            
        }
        
        int intResult           = 0;
        double doubleResult     = 0.0;
        
        SFA->computeMe->Apply(*last, intResult, doubleResult);
        sum += intResult + doubleResult;
        
        delete prev;
        
    }
    
    sprintf(double_string, "%f|",sum);
    
    buffer.ComposeRecord(&out_sch, double_string); 
    
    int totAttsToKeep = SFA->groupAtts->numAtts + 1;
    int attsToKeep[totAttsToKeep];
    attsToKeep[0] = 0; 
    for(int i = 1; i < totAttsToKeep; i++)
    {
        attsToKeep[i] = SFA->groupAtts->whichAtts[i-1];
    }
    Record final_rec;
    final_rec.MergeRecords(&buffer, last , 1, totAttsToKeep - 1, attsToKeep, totAttsToKeep, 1);
    
    SFA->outPipe->Insert(&final_rec);
    
    SFA->outPipe->ShutDown ();
}

void WriteOut :: Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) 
{ 
    SFA = new SelectFileArgs(inPipe, outFile, mySchema);
    pthread_create (&thread, NULL, WriteOutWorker, (void *)SFA);
}

void WriteOut :: WaitUntilDone () 
{ 
    pthread_join (thread, NULL);
}

void WriteOut :: Use_n_Pages (int n) 
{ 
    
}

void * WriteOutWorker (void * SFileArgs)
{
    SelectFileArgs * SFA = (SelectFileArgs *) SFileArgs;
    
    Record buffer;
    int cnt=0;
    while (SFA->inPipe->Remove (&buffer))
    {
		cnt++;
		buffer.Print(SFA->mySchema);
        buffer.FilePrint(SFA->outFile, SFA->mySchema);
    }
    printf("No of records removed : %d",cnt);
}
