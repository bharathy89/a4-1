
#include <string.h>
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include <stdlib.h>
#include "Defs.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

typedef struct MetaData {
	//public:
	char filename[20];
	int type;
	//int bits[(2*MAX_ANDS) + 1];
};

int main () {
	int filedes;
	MetaData data;
	std::vector<MetaData> metaList;
	strcpy(data.filename,"Akhil");
	data.type=1;
	metalist.push_back(data);
	strcpy(data.filename,"bharath");
	data.type=0;
	metalist.push_back(data);
	strcpy(data.filename,"praneeth");
	data.type=1;
	metalist.push_back(data);
	strcpy(data.filename,"vinayak");
	data.type=1;
	metalist.push_back(data);
	filedes = open("metafile.bin", O_TRUNC | O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	lseek (filedes,0,SEEK_SET);
	int size = metalist.size();
	write (filedes,&size,sizeof(int));
	int i;	
	for(i=0;i<size;i++) {
		MetaData * dream = metalist[i];
		write(filedes,dream,sizeof(MetaData));
	}
	close(filedes);
	int newsize;
	filedes = open("metafile.bin", O_RDWR, S_IRUSR | S_IWUSR);
	lseek(filedes,0,SEEK_SET);
	read(filedes,&newsize,sizeof(int));
	for(i=0;i<newsize;i++) {
		MetaData * walker = (MetaData * walker)malloc(sizeof(MetaData));
		read(filedes,walker,sizeof(MetaData));
		printf("filename : %s",walker->filename);
		printf("type : %d",walker->type);
		delete walker;
	}
	close(filedes);
	//sortorder.GenerateBytes(data.bits);
	/*int i;
	for(i=0;i<2*MAX_ANDS +1 ;i++) {
		printf("\n%d %d",i,data.bits[i]);
	}
	ofstream ofs("metafile.bin", ios::binary);
	ofs.write((char *)&data, sizeof(data));
	ofs.close();

	MetaData outData;
	ifstream ifs("metafile.bin", ios::binary);

	ifs.read((char *)&outData, sizeof(outData));
		
	printf("filename : %s",outData.filename);
	printf("type : %d",outData.type);
	for(i=0;i<2*MAX_ANDS +1 ;i++) {
		printf("\n%d %d",i,outData.bits[i]);
	}	
	
	ifs.read((char *)&outData, sizeof(outData));	
	
	// now open up the text file and start procesing it
       // FILE *tableFile = fopen ("/Users/bharatyarlagadda/Desktop/lineitem.tbl", "r");

        //Record temp;
        //Schema mySchema ("catalog", "lineitem");

	//char *bits = literal.GetBits ();
	//cout << " numbytes in rec " << ((int *) bits)[0] << endl;
	//literal.Print (&supplier);

        // read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	/*int counter = 0;
	ComparisonEngine comp;
        while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}

		//if (comp.Compare (&temp, &literal, &myComparison))
        //        	temp.Print (&mySchema);

        }
		cout << "counter : "<< counter;*/
}


