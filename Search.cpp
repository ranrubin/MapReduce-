
#include "MapReduceClient.h"
#include "MapReduceFramework.h"


#define SEARCH_STR_IDX 1
#define MUL_TH_LVL 10
#define ERROR_MSG "Usage: <substring to search><folders, separated by space>"
#define EXIT_FAILURE -1



#include <iostream>
#include <dirent.h>
#include <stdlib.h>
#include <cstring>
#include <map>
#include <algorithm>


//TOOD: delete
typedef std::vector<IN_ITEM> IN_ITEMS_VEC;


/**
 * A struct comparator between two k2Base objects
 */
typedef struct K2Comp
{
    bool operator()(k2Base * k1, k2Base * k2){
        return *k1 < *k2;
    }
} K2_COMPARATOR;

/**
 * A class representing the directory path recieved as input, and is inheriting
 * from k1Base.
 */
class InputName : public k1Base {

private:
    std::string dirname;
public:

    InputName(std::string name){
        dirname = name;
    }

    std::string getName() const {
        return dirname;
    }

    ~InputName(){
    }

    bool operator<(const k1Base &other) const{

        return dirname < dynamic_cast<const InputName &>(other).getName();
    }


};

/**
 * A class representing the search word recieved as input, and is inheriting
 * from v1Base.
 */
class SearchWord : public v1Base{

private:
    std::string searchWord;
public:

    SearchWord(std::string word){
        searchWord = word;
    }

    std::string getSearchWord() const{
        return searchWord;
    }

    ~SearchWord(){
    }

    bool operator<(const v1Base &other) const{
        return searchWord < dynamic_cast<const SearchWord&>(other).getSearchWord();
    }

};



/**
 * A class representing a file name, and is inheriting from k2base and k3base.
 */
class FileName : public k2Base, public k3Base{

private:
    std::string fileName;
public:

    FileName(std::string word){
        fileName = word;
    }

    std::string getFileName() const{
        return fileName;
    }

    ~FileName(){
    }

    // comparators for k2Base and k3Base objects
    bool operator<(const k3Base &other) const
    {
        return fileName < dynamic_cast<const FileName &>(other).getFileName();
    }
    bool operator<(const k2Base &other) const
    {
        return fileName < dynamic_cast<const FileName &>(other).getFileName();
    }

};


/**
 * A class representing the amount of time a certain filename was found,
 * and it's inheriting from v3base and v2base.
 */
class Count : public v3Base, public v2Base{

private:
    int count;
public:

    Count(){
        count = 0;
    }

    Count(int a){
        count = a;
    }

    int getCount() const{
        return count;
    }


    ~Count(){
    }

    bool operator<(const v3Base &other) const{
        return count < dynamic_cast<const Count&>(other).getCount();
    }
    bool operator<(const v2Base &other) const{
        return count < dynamic_cast<const Count&>(other).getCount();
    }

};


/**
 * A method responsible for initiating the starting IN_ITEMS_VEC list, which
 * constructs of a tuples of a folder path and the search word recieved
 * @param arg number of args received
 * @param argv the list of args
 * @return IN_ITEMS_VEC with the tuples of data described above
 */
IN_ITEMS_VEC initiateList(int arg, char* argv[]) {

    // declaring vars for main
    IN_ITEMS_VEC items_vec;
    for (int i = 2; i < arg; i++) {
        SearchWord * s = new SearchWord(argv[SEARCH_STR_IDX]);
        InputName * in = new InputName(argv[i]);

        std::pair<k1Base *, v1Base*> pair = std::make_pair(in, s);
        items_vec.push_back(pair);
    }
    return items_vec;
}


/**
 * The implementation of the MapReduce class, as needed in order to implement
 * the search engine. It constructs of an implementation for both the Map
 * and the Reduce methods which will be used by our framework later on.
 */
class MapReduce : public MapReduceBase {
public:

    /**
     * empty constructor
     */
    MapReduce(){};

    /**
     * gets a directory and search word, sends to emit 2 the directory name
     * and files which the search word is a part of
     * @param key - a directory
     * @param val - a search word
     */
    void Map(const k1Base *const key, const v1Base *const val) const override {

        struct dirent *ent;
        DIR *dir;

        InputName const * dirName = dynamic_cast<const InputName *>(key);
        SearchWord const * search = dynamic_cast<const SearchWord *>(val);
        if((dir = opendir(dirName->getName().c_str())) != NULL)
        {
            while ((ent = readdir (dir)) != NULL) {
                if (!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..") ) {
                    // filtering the dots while looping the ent struct files
                    continue;
                }

                std::string str(ent->d_name);

                if (str.find(search->getSearchWord()) != std::string::npos) {
                    FileName * k2 = new FileName(ent->d_name);
                    // converting const dirName/search to non-const
                    Count * v2 = new Count(1);
                    Emit2(k2, v2);
                }
            }
            closedir(dir);
        }
    }

    /**
     * gets a directory and vector of files
     * @param key  - a directory
     * @param vals  - vecor of files
     */
    void Reduce(const k2Base *const key, const V2_VEC &vals) const{
        FileName const * file = dynamic_cast<const FileName *>(key);
        FileName * k3 = new FileName(file->getFileName());
        Count * v3 = new Count((int)vals.size());
        Emit3(k3, v3);
    }
};

/**
 * cleans up the allocated data accordingly
 * @param v1 the initial vector we created, to be cleaned
 * @param v3 the out items vector, to be cleaned
 */
void cleanUp(IN_ITEMS_VEC v1, OUT_ITEMS_VEC v3){

    for (unsigned long i=0; i< v1.size(); i++){
        delete(v1[i].first);
        delete(v1[i].second);
    }

    for (unsigned long i=0; i< v3.size(); i++){
        delete(v3[i].first);
        delete(v3[i].second);
    }


}

/**
 * main program. constructs the initial vector, sending it to the framework and
 * printing results accordingly
 * @param arg number of args received
 * @param argv the list of args
 * @return 0 upon success
 */
int main(int arg, char* argv[]) {
    if (arg < 2) {
        std::cerr << ERROR_MSG;
        exit(EXIT_FAILURE);
    }



    // running the initiate
    IN_ITEMS_VEC items_vec = initiateList(arg, argv);

    MapReduce map;
    OUT_ITEMS_VEC out_items_vec = RunMapReduceFramework
            (map, items_vec, MUL_TH_LVL, true);


    std::string tempFileName;
    int tempCount;
//    std::cout << "out vector size: " << out_items_vec.size() << std::endl;
    for(unsigned long i=0; i < out_items_vec.size(); i++) {
        const FileName* temp = dynamic_cast<const FileName *>(out_items_vec[i].first);
        const Count* count = dynamic_cast<const Count *>(out_items_vec[i].second);
        tempCount = count->getCount();
        tempFileName = temp->getFileName();
        //std::cout << "tempCount: " << tempCount << std::endl;
        for (int j=0; j < tempCount; j++) {
            std::cout << tempFileName;
            if (i < out_items_vec.size() - 1) {
                std::cout << " ";
            }
        }
    }
    std::cout << std::endl;

    cleanUp(items_vec, out_items_vec);
    return 0;
}

