#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <string>
#include <vector>
#include <map>
#include <utility>
#include <functional>
#include <memory>
#include <cstring>
#include <cmath>
#include <iostream>
#include <fstream>

using namespace std;

class RelationData
{
    double numberOfTuples;
    int partition;
    
    public:
    
    map<string, int> attributeMap;  
    
    RelationData (double numTuples) : numberOfTuples(numTuples) {
        attributeMap.clear();
        partition = -1;
    }
        
    ~RelationData() {
        attributeMap.clear();
    }
    
   
    double getNumTuples() {
        return numberOfTuples; 
    }

    void setNumTuples(double val) {
        numberOfTuples = val;
    }

    void setAttribute(char * attName, int numDistincts) {
        string key (attName);
        int curr_val;
    
        if (numDistincts == -1) {
            curr_val = numberOfTuples;
        } else {
            curr_val = numDistincts;
        }
    
        if (attributeMap.find(key) == attributeMap.end()) {
            attributeMap.insert(pair<string, int>(key,curr_val));
        } else {
            attributeMap.find(key)->second = curr_val;
        }
    }

    void printAttribute() {
        for(map<string, int>::iterator it = attributeMap.begin(); it != attributeMap.end(); ++it) {
            cout << endl << it->first << "\n";
        }
    } 

    int getPartition() {
        return partition; 
    }

    void setPartition(int val) {
        partition = val;
    }  
};

class Statistics
{
    vector<string> relationNamesVector;
    map<string, RelationData> relationMap;
    map<string, double> relationTuplesMap;
    map<string, int>::iterator attMapIt;
    
    
    public:
        static int partitionCount;
        
        Statistics();
        Statistics(Statistics &copyMe);  // Performs deep copy
        ~Statistics();

        void AddRel(char *relName, int numTuples);
        void AddAtt(char *relName, char *attName, int numDistincts);
        void CopyRel(char *oldName, char *newName);

        void Read(char *fromWhere);
        void Write(char *fromWhere);
        void Print();

        void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
        double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
        int validator(int numToJoin, char * attrName);
};

#endif
