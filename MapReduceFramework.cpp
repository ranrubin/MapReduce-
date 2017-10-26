#include "MapReduceFramework.h"
#include <pthread.h>
#include <map>
#include <fstream>
#include <sys/time.h>
#include <ctime>
#include <semaphore.h>
#include <stdlib.h>
#include <iostream>
#include <libltdl/lt_system.h>
#include <algorithm>




#define DATA_CHUNK 10
#define SHUFFLE_SEM_VAL 0


#define LOG_NAME "/.MapReduceFramework.log"
//todo:change

#define SEC_TO_NAN 1000000000.0
#define MIC_TO_NAN 1000.0
#define PRINT_MA_SH 1
#define PRINT_REDUCE 0



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
 * A comparator function between two pointers to K3 object
 * @param k1 - a pointer to k3Base obj
 * @param k2 - a pointer to k3Base obj
 * @return true if k1 < k2
 */
static bool k3compare(OUT_ITEM k1, OUT_ITEM k2){
    return *(k1.first) < *(k2.first);
}


//helpful typedefs
typedef std::pair<k2Base*, v2Base*> MAP_OUT;
typedef std::pair<k3Base*, v3Base*> REDUCE_OUT;
typedef std::pair<k2Base*, V2_VEC> REDUCE_IN;

//Structs that will encapsulate the time information
static struct timeval startMap;
static struct timeval endMap;
static struct timeval startReduce;
static struct timeval endReduce;



//global variables
// queue container indexes, which indicates the next value to read
static int itemsContainerIndex;
static int shuffleContainerIdx;
//A boolean stating if the execMap threads are done
static bool execMapIsDone;
//A pointer to MapReduceBase that will allow us to use its functions
static MapReduceBase *curMapReduce;
static std::ofstream logFile;

//global locks
static pthread_mutex_t reduceContainerMutex;
static pthread_mutex_t shuffleContainerMutex;
static pthread_mutex_t containerMutex;
static pthread_mutex_t reduceMutex;
static pthread_mutex_t indexOfTotalItems;
static pthread_mutex_t indexOfDoneReduce;
static pthread_mutex_t logFileMutex;
static sem_t shuffleSem;


//global containers

//holds the information that each execMap produces
static std::map<pthread_t , std::vector<MAP_OUT>> pthreadToContainer;
//holds the information that each execReduce produces
static std::map<pthread_t , std::vector<REDUCE_OUT>> pthreadToReduceContainer;
//The container used by shuffle to organize the data
static std::map<k2Base*, V2_VEC, K2_COMPARATOR> shuffleMap;
//This vector will hold the shuffle map data
static std::vector<REDUCE_IN> shuffleVec;
//holds a mutex for each execMap thread
static std::map<pthread_t , pthread_mutex_t> execLocks;
//A vector holds the preliminary data
static IN_ITEMS_VEC totalItemsVec;


/**
 * This function returns the time pattern as requested [DD.MM.YYYY HH:MM:SS]
 * @return - a string as described
 */
static std::string getTime(){

    std::string str = "[";
    time_t t = time(NULL);
    tm *ltm = localtime(&t);
    int temp = ltm->tm_mday;
    if (temp < 10){
        str += "0";
    }
    str+= std::to_string(temp);
    str += ".";

    temp = 1 + ltm->tm_mon;
    if (temp < 10){
        str += "0";
    }
    str+= std::to_string(temp);
    str += ".";

    temp =1900 + ltm->tm_year;
    str+= std::to_string(temp);
    str += " ";

    temp = ltm->tm_hour;
    if (temp < 10){
        str += "0";
    }
    str+= std::to_string(temp);
    str += ":";

    temp = ltm->tm_min;
    if (temp < 10){
        str += "0";
    }

    str+= std::to_string(temp);
    str += ":";

    temp = ltm->tm_sec;
    if (temp < 10){
        str += "0";
    }

    str+= std::to_string(temp);
    str += "]\n";

    return str;

}


/**
 * checks if an action was prformed seccessfully (rc is an indicator), if
 * not, produces the given msg and exits
 * @param rc
 * @param funcName
 */
static void validateSuccess(int rc, std::string funcName) {
    if (rc) {
        std::cerr << "MapReduceFramework Failure: "<< funcName <<" failed.";
        exit(EXIT_FAILURE);
    }
}


/**
 * This function produces to the log file the msg it was given
 * @param msg
 */
static void informCreationTermination(std::string msg){
    int rc;
    rc = pthread_mutex_lock(&logFileMutex);
    validateSuccess(rc, "pthread_mutex_lock");
    logFile << msg << getTime();
    rc = pthread_mutex_unlock(&logFileMutex);
    validateSuccess(rc, "pthread_mutex_unlock");
}

/**
 * This function prints to log the total time it took for two different
 * functions (flag indicates which), Map&Shuffle and Reduce. all in nano sex
 * @param flag - if PRINT_MA_SH, prints the time it took for map and
 * shuffle, else prints the time the reduce and output took
 */
static void printTotalTime(int flag){
    int rc;
    double calc;

    if (flag == PRINT_MA_SH){ //shuffle and execMap are done
        calc = (endMap.tv_sec  - startMap.tv_sec)*SEC_TO_NAN +
               (endMap.tv_usec - startMap.tv_usec)*MIC_TO_NAN;
        rc = pthread_mutex_lock(&logFileMutex);
        validateSuccess(rc, "pthread_mutex_lock");
        logFile << std::string("Map and Shuffle took ") << calc <<
                std::string("ns\n");
        rc = pthread_mutex_unlock(&logFileMutex);
        validateSuccess(rc, "pthread_mutex_unlock");
    }
    else{
        //this action will be called last so the log is closed at the end
        calc = (endReduce.tv_sec  - startReduce.tv_sec)*SEC_TO_NAN +
               (endReduce.tv_usec - startReduce.tv_usec)*MIC_TO_NAN;
        rc = pthread_mutex_lock(&logFileMutex);
        validateSuccess(rc, "pthread_mutex_lock");
        logFile << std::string("RunMapReduceFramework finished\n");
        logFile << std::string("Reduce took ") << calc << std::string("ns\n");
        logFile << std::string("\n");
        logFile.close();
        rc = pthread_mutex_unlock(&logFileMutex);
        validateSuccess(rc, "pthread_mutex_unlock");
    }

}


/**
 * This function is called by the map function that was implemnted by user.
 * it adds the data into the global data base
 * @param k2 - k2Base obj
 * @param v2 - v2Base obj
 */
void Emit2 (k2Base* k2, v2Base* v2){
    pthreadToContainer[pthread_self()].push_back(std::make_pair(k2, v2));

}


/**
 * This function is called by the Reduce function that was implemnted by user.
 * it adds the data into the global data base
 * @param k3 - k3Base obj
 * @param v3 - v3Base obj
 */
void Emit3 (k3Base* k3, v3Base* v3){

    pthreadToReduceContainer[pthread_self()].push_back(std::make_pair(k3, v3));

}


/**
 * This function returns the next availabe index and increments the global
 * index by DATA_CHUNK
 * @return - the next availabe index
 */
static int getNextIdxMap(){
    int rc, curIdx;
    rc = pthread_mutex_lock(&indexOfTotalItems);
    validateSuccess(rc, "pthread_mutex_lock");
    curIdx = itemsContainerIndex;
    itemsContainerIndex += DATA_CHUNK;
    rc = pthread_mutex_unlock(&indexOfTotalItems);
    validateSuccess(rc, "pthread_mutex_unlock");
    return curIdx;
}

/**
 * This function runs once a thread is spawned. each thread goes over the
 * totalItemsVec, takes a chunck of data, manipulate it and saves it in its
 * own data structure. after each adding, it wakes up the shuffle semaphore
 */
static void * execMap(void *){

    int rc, curIdx;
    unsigned long itemsSize, endloop;

    itemsSize = totalItemsVec.size(); // this is the upper bound of the indexes

    //waiting for the pthreadToContainer to be initialize
    rc = pthread_mutex_lock(&containerMutex);
    validateSuccess(rc, "pthread_mutex_lock");
    rc = pthread_mutex_unlock(&containerMutex);
    validateSuccess(rc, "pthread_mutex_unlock");


    // starting execMap mapping
    while (true) {
        curIdx = getNextIdxMap();
        endloop = (unsigned long)(curIdx + DATA_CHUNK);
        if (curIdx > (int) itemsSize) { // there is nothing to read
            informCreationTermination("Thread ExecMap terminated ");
            pthread_exit(NULL);
        }
        if (endloop > itemsSize) { //end of container but some values are left
            endloop = itemsSize;
        }

        for (int i = curIdx; i < (int) endloop; i++) {
            //locking the shared container with shuffle so there will be no
            // conjunctions
            rc = pthread_mutex_lock(&execLocks[pthread_self()]);
            validateSuccess(rc, "pthread_mutex_lock");
            //calling map
            curMapReduce->Map(totalItemsVec[i].first, totalItemsVec[i].second);
            rc = pthread_mutex_unlock(&execLocks[pthread_self()]);
            validateSuccess(rc, "pthread_mutex_unlock");
            //incrementing semaphore to wake up shuffle
            rc = sem_post(&shuffleSem);
            validateSuccess(rc, "sem_post");
        }
    }
}

/**
 * This function returns the next availabe index and increments the global
 * index by DATA_CHUNK
 * @return - the next availabe index
 */
static int getNextRedIdx(){
    int rc, curIdx;
    rc = pthread_mutex_lock(&shuffleContainerMutex);
    validateSuccess(rc, "pthread_mutex_lock");
    curIdx = shuffleContainerIdx;
    shuffleContainerIdx += DATA_CHUNK;
    rc = pthread_mutex_unlock(&shuffleContainerMutex);
    validateSuccess(rc, "pthread_mutex_unlock");
    return curIdx;
}

/**
 * This function runs once a thread is spawned. each thread goes over the
 * shuffleVec, takes a chunck of data, manipulate it and saves it in its
 * own data structure.
 */
static void * execReduce(void *){

    int rc, curIdx;
    unsigned long itemsSize, endloop;

    itemsSize = shuffleVec.size();

    //waiting for the pthreadToReduceContainer to be initialize
    rc = pthread_mutex_lock(&reduceContainerMutex);
    validateSuccess(rc, "pthread_mutex_lock");
    rc = pthread_mutex_unlock(&reduceContainerMutex);
    validateSuccess(rc, "pthread_mutex_unlock");

    while (true) {
        curIdx = getNextRedIdx();
        endloop = (unsigned long)(curIdx + DATA_CHUNK);

        if (curIdx > (int)itemsSize) { //nothing more left to read
            informCreationTermination("Thread ExecReduce terminated ");
            pthread_exit(NULL);
        }

        if (endloop > itemsSize) { //end of container but some values are left
            endloop = itemsSize;
        }
        for (int i = curIdx; i < (int) endloop; i++) {
            // applying reduce
            curMapReduce->Reduce(shuffleVec[i].first, shuffleVec[i].second);
        }
    }
}

/**
 * This function converts the global ShuffleMap to a vector for convinet use
 * later on
 */
static void convertToVec(){
    auto it = shuffleMap.begin();
    for (; it != shuffleMap.end(); it++){
        shuffleVec.push_back(std::make_pair(it->first, it->second));
    }
}


/**
 * This function starts running when the shuffle thread is spawned. The
 * shuffle waits to be awakened by execMap threads and once awakend ,
 * iterates over pthreadToContainer and pulls out the desired data.
 */
static void * shuffleThread(void *){

    int rc, sem_val, emptyRun = 0;
    unsigned long emptyCounter = 0;

    while(true){

        rc = sem_getvalue(&shuffleSem, &sem_val);
        validateSuccess(rc, "sem_getvalue");

        //if in the last run all the threads in pthreadToContainer hold an
        // empty data base, and the execMaps are done, that means an empty
        // run was done
        //TODO: check what it does
        if (execMapIsDone && (emptyCounter == pthreadToContainer.size())){
            emptyRun ++;
        }

        //if all the execMap threads exited and the sempahore value is 0 or
        // there were more than 2 empty runs, that means that all the data
        // was read fro, pthreadToContainer data base
        if ((execMapIsDone) && ((emptyRun > 2) || (sem_val == 0))){
            informCreationTermination("Thread Shuffle terminated ");
            //converting the shuffleMap to vector
            convertToVec();
            pthread_exit(NULL);
        }

        //waiting to be awakened
        rc = sem_wait(&shuffleSem);
        validateSuccess(rc, "sem_wait");

        emptyCounter = 0;
        auto iter = pthreadToContainer.begin();
        //iterating over pairs of <pid, vector<k2Base, v2Base>>
        for(; iter != pthreadToContainer.end(); iter++){

            if (!iter->second.empty()){ //if the thread's shuffle has data

                //locking that thread's specific container
                rc = pthread_mutex_lock(&execLocks[iter->first]);
                validateSuccess(rc, "pthread_mutex_lock shuffle");

                //iterating over the thread's data, that is , a vector of
                // pairs holding <k2Base, v2Base>
                for (int i = 0; i < (int) iter->second.size(); i++){
                    MAP_OUT pair = iter->second[i];
                    shuffleMap[pair.first].push_back(pair.second);
                }
                //clearing the data that the shuffle already took
                iter->second.clear();
                rc = pthread_mutex_unlock(&execLocks[iter->first]);
                validateSuccess(rc, "pthread_mutex_unlock");

            } else {
                emptyCounter ++;
            }
        }
    }
}

/**
 * freeing all memory that was allocated in the program, and if the falg is
 * true deleting the k2 v2 as well
 * @param autoDeleteV2K2  - a flag that indicates if we need to delete v2 k2.
 */
static void cleanUp(bool autoDeleteV2K2){
    int rc;
    if (autoDeleteV2K2){
        try {
            for (int i = 0; i < (int) shuffleVec.size(); i++){

                for (int j = 0; j < (int) shuffleVec[i].second.size(); j++){
                    delete(shuffleVec[i].second[j]);
                }
                delete(shuffleVec[i].first);
            }
        }
        catch (...){
            validateSuccess(1, "delete");
        }

    }

    rc = pthread_mutex_destroy(&reduceContainerMutex);
    validateSuccess(rc, "pthread_mutex_destroy");
    rc = pthread_mutex_destroy(&shuffleContainerMutex);
    validateSuccess(rc, "pthread_mutex_destroy");
    rc = pthread_mutex_destroy(&containerMutex);
    validateSuccess(rc, "pthread_mutex_destroy");
    rc = pthread_mutex_destroy(&reduceMutex);
    validateSuccess(rc, "pthread_mutex_destroy");
    rc = pthread_mutex_destroy(&indexOfTotalItems);
    validateSuccess(rc, "pthread_mutex_destroy");
    rc = pthread_mutex_destroy(&indexOfDoneReduce);
    validateSuccess(rc, "pthread_mutex_destroy");
    rc = sem_destroy(&shuffleSem);
    validateSuccess(rc, "sem_destroy");

}

/**
 * This function is responsible to produce the final output and freeing all
 * memory that was allocated during the entire run.
 * @param autoDeleteV2K2 - this flag inidicates if there is a need to free
 * athe memory that was allocated by the user
 * @return OUT_ITEMS_VEC containing the desired data
 */
static OUT_ITEMS_VEC produceFinalOutput(bool autoDeleteV2K2){
    OUT_ITEMS_VEC output;

    //collecting all the data from the reduce threads
    for(auto thread : pthreadToReduceContainer){
        for (auto pair : thread.second) {
            output.push_back(pair);
        }
    }

    std::sort(output.begin(), output.end(), k3compare);
    //releasing all data
    cleanUp(autoDeleteV2K2);
    int endTimeInd = gettimeofday(&endReduce, NULL);
    validateSuccess(endTimeInd, "gettimeofday");
    printTotalTime(PRINT_REDUCE);
//    endTimeInd = pthread_mutex_destroy(&logFileMutex);
//    validateSuccess(endTimeInd, "pthread_mutex_destroy");
    return output;
}

/**
 * initiating all mutexes
 */
static void initiateLocks(){
    int rc;

    rc = pthread_mutex_init(&reduceContainerMutex, NULL);
    validateSuccess(rc, "pthread_mutex_init");
    rc = pthread_mutex_init(&shuffleContainerMutex, NULL);
    validateSuccess(rc, "pthread_mutex_init");
    rc = pthread_mutex_init(&logFileMutex, NULL);
    validateSuccess(rc, "pthread_mutex_init");
    rc = pthread_mutex_init(&reduceMutex, NULL);
    validateSuccess(rc, "pthread_mutex_init");
    rc = pthread_mutex_init(&containerMutex, NULL);
    validateSuccess(rc, "pthread_mutex_init");
    rc = pthread_mutex_init(&indexOfTotalItems, NULL);
    validateSuccess(rc, "pthread_mutex_init");
    rc = pthread_mutex_init(&indexOfDoneReduce, NULL);
    validateSuccess(rc, "pthread_mutex_init");
    rc = sem_init(&shuffleSem, 0, SHUFFLE_SEM_VAL);
    validateSuccess(rc, "sem_init");
}




/**
 * This function creates the execmap threads.
 * @param multiThreadLevel - number of desired threads
 * @param attr - a joinable attribute
 */
static void createExecMapThreads(int multiThreadLevel, pthread_attr_t* attr){
    int rc;
    //void * status;
    pthread_t mapThreads[multiThreadLevel];

    //locking the container mutex so the threads won't start producing data
    // until all threads were inserted into the containers
    rc = pthread_mutex_lock(&containerMutex);
    validateSuccess(rc, "pthread_mutex_lock");

    for (int i=0; i < multiThreadLevel; i++){
        rc = pthread_create(&mapThreads[i], attr, execMap, NULL);
        validateSuccess(rc, "pthread_create");
        informCreationTermination("Thread ExecMap created ");
        //initiating containers that need the threads id
        pthreadToContainer[mapThreads[i]] = std::vector<MAP_OUT>();
        rc = pthread_mutex_init(&execLocks[mapThreads[i]], NULL);
        validateSuccess(rc, "pthread_mutex_init");
    }

    //unlocking the container mutex
    rc = pthread_mutex_unlock(&containerMutex);
    validateSuccess(rc, "pthread_mutex_unlock");

    //joining the calling thread to the execmap's so it won't exit this
    // function until all execMap are dead
    for (int i=0; i<multiThreadLevel; i++){
        rc = pthread_join(mapThreads[i], NULL);
        validateSuccess(rc, "pthread_join");
    }
}

/**
 * This fucntion creates the exec reduce threads and waits for them to be
 * donr before returning.
 * @param multiThreadLevel - desired number of threads
 * @param attr - a JOINABLE attribute
 */
static void createExecReduceThreads(int multiThreadLevel, pthread_attr_t*
attr){
    void * status;
    int rc;
    pthread_t reduceThreads[multiThreadLevel];

    //locking the mutex so the threads won't run befor the container is ready
    rc = pthread_mutex_lock(&reduceContainerMutex);
    validateSuccess(rc, "pthread_mutex_lock");

    //creating all of the threads joinable
    for (int i = 0; i < multiThreadLevel; i++){
        rc = pthread_create(&reduceThreads[i], attr, execReduce, NULL);
        validateSuccess(rc, "pthread_create");
        informCreationTermination("Thread ExecReduce created ");
        //initializing threads container
        pthreadToReduceContainer[reduceThreads[i]] = std::vector<REDUCE_OUT>();
    }

    //unlocking this mutex allowas execReduce threads to continue their run
    rc = pthread_mutex_unlock(&reduceContainerMutex);
    validateSuccess(rc, "pthread_mutex_unlock");

    //making the calling func to wait for all the reduce threads to finish.
    for (int i = 0; i < multiThreadLevel; i++){
        rc = pthread_join(reduceThreads[i], &status);
        validateSuccess(rc, "pthread_join");
    }
}



/**
 *This is the main function of the library. this functon runs a framework
 * for processing parallelizable problems.
 * @param mapReduce - An object that holds user's Map and Reduce functions
 * @param itemsVec - A vector holding the input data
 * @param multiThreadLevel - number of threads that we wish to use
 * @param autoDeleteV2K2 - A flag pointing if the framework should free
 * allocated memory.
 * @return - vector of k3Base* v3Base*
 */
OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){

    execMapIsDone = false;
    int rc, startTimeInd, endTimeInd;
    void * status;
    startTimeInd = gettimeofday(&startMap, NULL);
    validateSuccess(startTimeInd, "gettimeofday");

    //initiating some global variables
    totalItemsVec = itemsVec;
    curMapReduce = &mapReduce;
    itemsContainerIndex = 0;
    shuffleContainerIdx = 0;


    logFile.open(LOG_NAME, std::ios::app | std::ios::out);
    if (!logFile.is_open()){
        validateSuccess(1, "open");
    }

    logFile << std::string("RunMapReduceFramework started with ") <<
            multiThreadLevel << std::string(" threads\n");


    //initiating all the mutexes
    initiateLocks();




    //creating a joinable attribute
    pthread_attr_t attr;
    rc = pthread_attr_init(&attr);
    validateSuccess(rc, "pthread_attr_init");
    rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    validateSuccess(rc, "pthread_attr_setdetachstate");


    //creating the shuffle thread
    pthread_t shuffle;
    rc = pthread_create(&shuffle, &attr, shuffleThread, NULL);
    validateSuccess(rc, "pthread_create");
    informCreationTermination("Thread Shuffle created ");

    //creating execMap threads
    createExecMapThreads(multiThreadLevel, &attr);

    //execMap were joined, thus if we got here all execMaps are done
    execMapIsDone = true;
    // waking shuffle up for the last time
    sem_post(&shuffleSem);


    //waiting shuffle thread to finish
    rc = pthread_join(shuffle, &status);
    validateSuccess(rc, "pthread_join");

    //Map and Shuffle are done, printing total time
    endTimeInd = gettimeofday(&endMap, NULL);
    validateSuccess(endTimeInd, "gettimeofday");
    printTotalTime(PRINT_MA_SH);

    //Starting reduce - taking time and creating threads
    startTimeInd = gettimeofday(&startReduce, NULL);
    validateSuccess(startTimeInd, "gettimeofday");
    createExecReduceThreads(multiThreadLevel, &attr);

    //will get to this line if all reduce threads are done (because in
    // previous func there is a join condition)

    //producing final output and releasing data
    return produceFinalOutput(autoDeleteV2K2);
}
