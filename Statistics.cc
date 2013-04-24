#include "Statistics.h"

int Statistics::partitionCount = 0;

Statistics::Statistics() {
}

Statistics::Statistics(Statistics &copyMe) {
    relationMap.insert(copyMe.relationMap.begin(), copyMe.relationMap.end());
}

Statistics::~Statistics() {
    relationMap.clear();
}

void Statistics::AddRel(char *relName, int numTuples) {
    string key (relName);
    
    if (relationMap.find(key) == relationMap.end() ) {
        RelationData relDetails(numTuples);
        relationMap.insert(pair<string, RelationData>(key, relDetails));
    } else  {
        relationMap.find(key)->second.setNumTuples(numTuples);
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
    string key (relName);
    relationMap.find(key)->second.setAttribute(attName, numDistincts);
}

void Statistics::CopyRel(char *oldName, char *newName) {
    string oldKey (oldName);
    string newKey (newName);
    
    RelationData relDetails(relationMap.find(oldKey)->second);
    relationMap.insert(pair<string, RelationData>(newKey, relDetails));
}

void Statistics::Read(char *fromWhere) {
    ifstream readFile (fromWhere);
    relationMap.clear();
    string start;
    string relName;
    readFile >> start;
    
    while (!strcmp(start.c_str(), "RelationBegin")) {
        start = "NULL";
        double tmp;
        string tmp_str;
        readFile >> relName;
        readFile >> tmp;
        AddRel(const_cast <char *> (relName.c_str()), tmp);
        readFile >> tmp_str;
        
        while ((strcmp(tmp_str.c_str(), "RelationEnd")) != 0) {
            readFile >> tmp;
            AddAtt(const_cast <char *> (relName.c_str()), const_cast <char *> (tmp_str.c_str()), tmp);
            readFile >> tmp_str;
        }
        readFile >> start;
    }
    readFile.close();
    
}

void Statistics::Write(char *fromWhere) {
    
    ofstream writeFile (fromWhere, ios_base::out | ios_base::trunc);
    
    for (map<string, RelationData> ::iterator mapIerator = relationMap.begin(); mapIerator != relationMap.end(); mapIerator++)  {
        writeFile << "RelationBegin" << endl;
        writeFile << mapIerator->first << endl;
        writeFile << mapIerator->second.getNumTuples() << endl;
        
        for (attMapIt = mapIerator->second.attributeMap.begin(); attMapIt != mapIerator->second.attributeMap.end(); attMapIt++)  {
            writeFile << attMapIt->first << endl;
            writeFile << attMapIt->second << endl;
            
        }
        writeFile << "RelationEnd" << endl;
    }
    
    writeFile.close();
    
    
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin) {
    relationNamesVector.clear();
    for (int i = 0 ; i < numToJoin; i++)  {
        relationNamesVector.push_back(string(relNames[i]));
    }

    double estimate = Estimate(parseTree, relNames, numToJoin);
    
    cout << "\nEstimate in Apply " << estimate << endl;
    
    for (int i = 0; i < numToJoin; i++)  {
        map<string, RelationData> ::iterator relMapIterator = relationMap.find(relationNamesVector[i]);
        relMapIterator->second.setNumTuples(estimate);
    }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin){
    relationNamesVector.clear();
    relationTuplesMap.clear();
    struct AndList *currentAnd;
    struct OrList *currentOr;
    bool join = false;
    double n = -1.0;
    for (int i = 0 ; i < numToJoin; i++) {
        relationNamesVector.push_back(string(relNames[i]));
    }
    
    for( currentAnd = parseTree; currentAnd != NULL ; currentAnd = currentAnd->rightAnd ) {
        double tmp_mult= 1.0;
        double tmp_n= -1.0;
        
        struct OrList *firstOr = currentOr;
        map<string, double> keyPresent;
        keyPresent.clear();
        for (currentOr = currentAnd->left; currentOr != NULL ; currentOr = currentOr->rightOr) {
            if ((currentOr->left->code == EQUALS) &&  ( (currentOr->left->left->code == NAME) && (currentOr->left->right->code == NAME) ) ) {
                join = true;
                
                map<string, RelationData> ::iterator relMapITLeft;
                map<string, RelationData> ::iterator relMapITRight;
                
                int relationNumLeft= -1;
                int relationNumRight = -1;
                
                relationNumLeft = validator(numToJoin, currentOr->left->left->value);
                relationNumRight = validator(numToJoin, currentOr->left->right->value);
                
                //cout << " relationNumLeft  " << relationNumLeft << endl;
                //cout << " relationNumRight  " << relationNumRight << endl;
                
                if  (( relationNumLeft == -1)  || 
                     ( relationNumRight == -1) ) { 
					
                    cout << "Attribute not found " << currentOr->left->left->value << " " << currentOr->left->right->value << endl;
                } else {
                    string tmpLeftAttr1  (currentOr->left->left->value);
                    string tmpRightAttr1 (currentOr->left->right->value);
                    
                    string tmpLeftAttr;
                    size_t foundLeft = tmpLeftAttr1.find('.');
                    if (foundLeft!=string::npos) {
                        tmpLeftAttr = tmpLeftAttr1.substr(foundLeft+1);
                    } else {
                        tmpLeftAttr = tmpLeftAttr1;
                    }
                    
                    string tmpRightAttr;
                    size_t foundRight = tmpRightAttr1.find('.');
                    if (foundRight!=string::npos) {
                        tmpRightAttr = tmpRightAttr1.substr(foundRight+1);
                    } else {
                        tmpRightAttr = tmpRightAttr1;
                    }
                    double totalTuplesLeft=-1.0;
                    double totalTuplesRight=-1.0;
                    double distinctLeft= -1.0;
                    double distinctRight=-1.0;
                    
                    relMapITLeft = relationMap.find(relationNamesVector[relationNumLeft]);
                    if ( relMapITLeft== relationMap.end() ) {
                        cout << "Relation not present left " << endl;
                    }
                    relMapITRight= relationMap.find(relationNamesVector[relationNumRight]);
                    
                    if ( relMapITRight== relationMap.end()) {
                        cout << "Relation not present right " << endl;
                    }
                    
                    totalTuplesLeft = relMapITLeft->second.getNumTuples();
                    if (relationTuplesMap.find(relationNamesVector[relationNumLeft]) != relationTuplesMap.end()) {
                        totalTuplesLeft = relationTuplesMap.find(relationNamesVector[relationNumLeft])->second;
                    }
                    totalTuplesRight = relMapITRight->second.getNumTuples();
                    if (relationTuplesMap.find(relationNamesVector[relationNumRight]) != relationTuplesMap.end()) {
                        totalTuplesRight = relationTuplesMap.find(relationNamesVector[relationNumRight])->second;
                    }
                    
                    attMapIt = relMapITLeft->second.attributeMap.find(tmpLeftAttr);
                    distinctLeft = attMapIt->second;
                    
                    //cout << "distinctLeft " << distinctLeft << endl;
                    
                    attMapIt = relMapITRight->second.attributeMap.find(tmpRightAttr);
                    distinctRight = attMapIt->second;
                    //cout << "distinctRight " << distinctRight << endl;
                    int partitionLeft = relMapITLeft->second.getPartition();
                    int partitionRight = relMapITRight->second.getPartition();
                    int newPartition = -1;
                    //cout << " total tuple left : " << totalTuplesLeft << endl;
                    //cout << " total tuple right : " << totalTuplesRight << endl;
                    double cartProduct= totalTuplesLeft * totalTuplesRight;
                    double maxDistinct= max(distinctLeft, distinctRight);
                    double joinEstimate= cartProduct / maxDistinct;
                    n = joinEstimate;
                    if ((partitionLeft  == partitionRight) && (partitionLeft == -1)) {
                        newPartition=partitionCount++;
                    } else if ( (partitionLeft == -1) || (partitionRight == -1) ) {
                        if (partitionLeft == -1) {
                            newPartition = relMapITRight->second.getPartition();
                        } else if (partitionRight == -1) {
                            newPartition = relMapITLeft->second.getPartition();
                        }
                    } else {
                        int leftPartition   = relMapITLeft->second.getPartition();
                        int rightPartition  = relMapITRight->second.getPartition();
                        newPartition = min(leftPartition, rightPartition);
                    }
                    relMapITLeft->second.setPartition(newPartition);
                    relMapITRight->second.setPartition(newPartition);
                    
                    for (map<string, RelationData> ::iterator relMapIterator = relationMap.begin(); 
                        relMapIterator != relationMap.end(); relMapIterator++) {
                        if (relMapIterator->second.getPartition() == newPartition) {
                            if (relationTuplesMap.find(relMapIterator->first) == relationTuplesMap.end()) {
                                relationTuplesMap.insert(pair<string, double>(relMapIterator->first, joinEstimate));
                            } else {
                                relationTuplesMap.find(relMapIterator->first)->second = joinEstimate;
                            }
                        }
                    }
                }
            } else {
                join                = false;
                int relationNum     = -1;
                double totalTuples  = -1;
                
                if ( ( relationNum  = validator(numToJoin, currentOr->left->left->value)) == -1) {
                    cout << "Attribute not found " << endl;
                } else {
                    map<string, RelationData> ::iterator relMapIterator = relationMap.find(relationNamesVector[relationNum]);
                    totalTuples = relMapIterator->second.getNumTuples();
                    
                    if (relationTuplesMap.find(relationNamesVector[relationNum]) != relationTuplesMap.end()) {
                        totalTuples = relationTuplesMap.find(relationNamesVector[relationNum])->second;
                    }
                    
                    //cout << "totalTuples" << totalTuples << endl;
                    
                    if ((int)n == -1) {
                        tmp_n = totalTuples;
                    } else {
                        tmp_n =n;
                    }

                    if (currentOr->left->code == EQUALS) {
                        double distinct = attMapIt->second;
                        if (keyPresent.find(attMapIt->first) == keyPresent.end()) {
                            keyPresent.insert(pair<string, double>(attMapIt->first, -1.0));
                            tmp_mult *= ( 1 - (1/distinct) );
                        } else {
                            double tmp_val;
                            if (keyPresent.find(attMapIt->first)->second == -1.0) {
                                tmp_val = 2.0 / distinct;
                                tmp_mult /= ( 1 - (1/distinct) );
                            } else if (keyPresent.find(attMapIt->first)->second == -2.0) {
                                tmp_val = (1.0/distinct) + (1.0/3.0);
                                tmp_mult /= ( 1 - (1.0/3.0) );
                            } else {
                                tmp_val = keyPresent.find(attMapIt->first)->second + (1.0/distinct);
                            }
                            keyPresent.find(attMapIt->first)->second = tmp_val;
                        }
                    } else if ( (currentOr->left->code == LESS_THAN) || (currentOr->left->code == GREATER_THAN) ) {
                        double distinct = attMapIt->second;
                        if (keyPresent.find(attMapIt->first) == keyPresent.end()) {
                            keyPresent.insert(pair<string, double>(attMapIt->first, -2.0));
                            tmp_mult *= ( 1.0 - (1.0/3.0) );
                        } else {
                            double tmp_val;
                            if (keyPresent.find(attMapIt->first)->second == -1.0) {
                                tmp_val = (1.0/distinct) + (1.0/3.0);
                                tmp_mult /= ( 1 - (1/distinct) );
                            } else if (keyPresent.find(attMapIt->first)->second == -2.0) {
                                tmp_val = 2.0 / 3.0;
                                tmp_mult /= ( 1 - (1.0/3.0) );
                            } else {
                                tmp_val = keyPresent.find(attMapIt->first)->second + (1.0/3.0);
                            }
                            keyPresent.find(attMapIt->first)->second = tmp_val;
                        }
                    }  
                }
            }
        }
        if (join == false) {
            map<string, double> ::iterator keyPresentIT;
            
            for (keyPresentIT = keyPresent.begin(); keyPresentIT != keyPresent.end() ; keyPresentIT++) {
                if ((keyPresentIT->second != -1.0) && (keyPresentIT->second != -2.0) ) {
                    double tmp_double_val = (1.0 - keyPresentIT->second);
                    tmp_mult *= tmp_double_val;
                }
            }
            n = tmp_n * ( 1 - tmp_mult );
            
            for (firstOr = currentAnd->left; firstOr != NULL ; firstOr = firstOr->rightOr) {
                string tmp (firstOr->left->left->value);
                
                size_t found=tmp.find('.');
                
                if (found!=string::npos) {
                    string relStr, attrStr;
                    
                    relStr = tmp.substr (0,found);
                    attrStr= tmp.substr (found+1);
                    
                    map<string, RelationData> ::iterator relMapIterator = relationMap.find(relStr);
                    attMapIt = relMapIterator->second.attributeMap.find(attrStr);
                    
                    if (  attMapIt != relMapIterator->second.attributeMap.end()){
                        if (relationTuplesMap.find(relMapIterator->first) == relationTuplesMap.end()) {
                            relationTuplesMap.insert(pair<string, double>(relMapIterator->first, n));
                        } else {
                            relationTuplesMap.find(relMapIterator->first)->second = n;
                        }
                    }
                } else {
                    for (int i = 0; i < numToJoin; i++) {
                        map<string, RelationData> ::iterator relMapIterator = relationMap.find(relationNamesVector[i]);
                        attMapIt = relMapIterator->second.attributeMap.find(tmp);
                        
                        if (  attMapIt != relMapIterator->second.attributeMap.end()) {
                            if (relationTuplesMap.find(relMapIterator->first) == relationTuplesMap.end()) {
                                relationTuplesMap.insert(pair<string, double>(relMapIterator->first, n));
                            } else {
                                relationTuplesMap.find(relMapIterator->first)->second = n;
                            }
                        }
                    } 
                }
            }
        }
    }
    return n;
}

int Statistics::validator (int numToJoin, char * attrName) {
    string tmp (attrName);
    size_t found=tmp.find('.');
    if (found!=string::npos) {
        string relStr, attrStr;
        relStr = tmp.substr (0,found);
        attrStr= tmp.substr (found+1);
        
        map<string, RelationData> ::iterator mapItr = relationMap.find(relStr);
        
        attMapIt = mapItr->second.attributeMap.find(attrStr);
    
        if (  attMapIt != mapItr->second.attributeMap.end()) {
            for (int i = 0; i < numToJoin; i++) {
                if (relStr.compare(relationNamesVector[i]) == 0){
                    return i;
                }
            }
        }
        
    } else {
        for (int i = 0; i < numToJoin; i++) {
            map<string, RelationData> ::iterator mapItr = relationMap.find(relationNamesVector[i]);
            attMapIt = mapItr->second.attributeMap.find(tmp);
            
            if (  attMapIt != mapItr->second.attributeMap.end()) {
                return i;
            }
        }
    }
    return -1;
}

/*
void Statistics::Print() {
    
    for (map<string, RelationData> ::iterator mapIerator = relationMap.begin(); mapIerator != relationMap.end(); mapIerator++)  {
        cout << "Relation Begin" << endl;
        cout << mapIerator->first << endl;
        cout << mapIerator->second.getNumTuples() << endl;
        
        for (attMapIt = mapIerator->second.attributeMap.begin(); attMapIt != mapIerator->second.attributeMap.end(); attMapIt++)  {
            cout << attMapIt->first << endl;
            cout << attMapIt->second << endl;
            
        }
        cout << "Relation End" << endl;
    }  
}
*/