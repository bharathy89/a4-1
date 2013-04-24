#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "Schema.h"
#include <cstring>
#include <string>
#include <vector>

using namespace std;


class SelectFileArgs {
    
public:
    
    DBFile      *inFile;
    Pipe        *inPipe;
    Pipe        *inPipeR;
	Pipe 		*outPipeL;
	Pipe 		*outPipeR;
    Pipe        *outPipe;
    CNF         *selOp;
    Record      *literal;
    int         *keepMe; 
    int         numAttsInput; 
    int         numAttsOutput;
    Function    *computeMe;
    Schema      *mySchema;
    Schema      *mySchemaR;
    OrderMaker  *groupAtts;
    FILE        *outFile;
    
    SelectFileArgs(DBFile &_inFile, Pipe &_outPipe, CNF &_selOp, Record &_literal)
    {
        inFile  = &_inFile;
        outPipe = &_outPipe;
        selOp   = &_selOp;
        literal = &_literal;
    }
    
    SelectFileArgs(Pipe &_inPipe, Pipe &_outPipe, int *_keepMe, int _numAttsInput, int _numAttsOutput)
    {
        inPipe          = &_inPipe;
        outPipe         = &_outPipe;
        keepMe          = _keepMe;
        numAttsInput    =  _numAttsInput;
        numAttsOutput   =  _numAttsOutput;
    }
    
    SelectFileArgs(Pipe &_inPipe, Pipe &_outPipe, Function &_computeMe)
    {
        inPipe          = &_inPipe;
        outPipe         = &_outPipe;
        computeMe       = &_computeMe;
    }
    
    SelectFileArgs(Pipe &_inPipe, Pipe &_output, Pipe &_outPipe, Schema &_mySchema)
    {
        inPipe          = &_inPipe;
		outPipeL		= &_output;
        outPipe         = &_outPipe;
        mySchema        = &_mySchema;
    }
    
    SelectFileArgs(Pipe &_inPipe, FILE *_outFile, Schema &_mySchema)
    {
        inPipe          = &_inPipe;
        outFile         = _outFile;
        mySchema        = &_mySchema;
    }
    
    SelectFileArgs(Pipe &_inPipe, Pipe &_output, Pipe &_outPipe, OrderMaker &_groupAtts, Function &_computeMe) 
    {
        inPipe          = &_inPipe;
		outPipeL		= &_output;		
        outPipe         = &_outPipe;
        computeMe       = &_computeMe;
        groupAtts       = &_groupAtts;
    }
    
    SelectFileArgs(Pipe &_inPipeL, Pipe &_inPipeR, Pipe &_outPipeL, Pipe &_outPipeR, Pipe &_outPipe, CNF &_selOp, Record &_literal, Schema *_mySchema, Schema *_mySchemaR)
    {
        inPipe      = &_inPipeL;
        inPipeR     = &_inPipeR;
		outPipeL    = &_outPipeL;
        outPipeR    = &_outPipeR;
        outPipe     = &_outPipe;
        selOp       = &_selOp;
        literal     = &_literal;
        mySchema    = _mySchema;
        mySchemaR   = _mySchemaR;
    }
    
    
};

class RelationalOp {
    
public:
    
    RelationalOp *parent;
    RelationalOp *left;
    RelationalOp *right;
    
    AndList *final;

    
    RelationalOp ()
    {
        parent  = NULL;
        left    = NULL;
        right   = NULL;
        final   = NULL;
    }
    
    pthread_t thread;
    SelectFileArgs *SFA;
    
        // blocks the caller until the particular relational operator 
        // has run to completion
        virtual void WaitUntilDone () = 0;
    
        // tell us how much internal memory the operation can use
        virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 
    
public:
    
    string  inFile, tableName;
    CNF     cnf;
    Record  literal;
    Schema  *mySchema;
    Pipe    *OutPipe;
    
    SelectFile(){}
    SelectFile(Schema *_schema) : mySchema(_schema)
    {
        
    }
        void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
        void WaitUntilDone ();
        void Use_n_Pages (int n);
    
};

void * SelectFileWorker (void * SFileArgs);

class SelectPipe : public RelationalOp {
    
public:
    
    string  tableName;
    CNF     cnf;
    Record  literal;
    Schema  *mySchema;
    
    SelectPipe(){}
    SelectPipe(Schema *_schema) : mySchema(_schema)
    {
        
    }
        void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { }
        void WaitUntilDone () { }
        void Use_n_Pages (int n) { }
};

class Project : public RelationalOp {
    
public:
    
        void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput,Schema *schema);
        void WaitUntilDone ();
        void Use_n_Pages (int n);
};

void * ProjectWorker (void * SFileArgs);

class Sum : public RelationalOp {
    
public:
    
        void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
        void WaitUntilDone ();
        void Use_n_Pages (int n);
};

void * SumWorker (void * SFileArgs);

class Join : public RelationalOp {
    
public:
    
    
    CNF     cnf;
    Record  literal;
    Schema  *leftSchema;
    Schema  *rightSchema;
    string  leftTableName;
    string  rightTableName;
    Pipe    *inPipeL, *inPipeR, *outPipe;
    Pipe outPipeL;
	Pipe outPipeR;
	char *one;
	char *two;
	OrderMaker left, right;
    Join():outPipeL (100), outPipeR (100){
		//outPipeL = new Pipe(100);
		one = (char *)malloc(4*sizeof(char));
		one = "one";
		two = (char *)malloc(4*sizeof(char));
		two = "two";
		//outPipeR = new Pipe(100);
	}
    Join (Schema *_l, Schema * _r, AndList *_f) : leftSchema(_l), rightSchema (_r), outPipeL(100), outPipeR(100)
    {
        final = _f;
    }
    
        void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal, Schema *mySchema, Schema *mySchemaR);
        void WaitUntilDone ();
        void Use_n_Pages (int n);
};

void * JoinWorker (void * SFileArgs);

class DuplicateRemoval : public RelationalOp {
    
public:
		OrderMaker *sortorder;
    	Pipe output;
		DuplicateRemoval():output(100) {
		};
        void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
        void WaitUntilDone ();
        void Use_n_Pages (int n);
};

void * DuplicateRemovalWorker (void * SFileArgs);

class GroupBy : public RelationalOp {

public:	

    Pipe output;
	GroupBy():output(100) {
	};
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

void * GroupByWorker (void * SFileArgs);

class WriteOut : public RelationalOp {
    
public:
    
        void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
        void WaitUntilDone ();
        void Use_n_Pages (int n);
};

void * WriteOutWorker (void * SFileArgs);
#endif
