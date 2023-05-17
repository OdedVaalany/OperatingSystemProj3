#include "MapReduceFramework.h";
#include <atomic>;
#include <algorithm>;
#include <semaphore.h>;
#include <unordered_map>;
#include <iostream>;
#include <math.h>;

std::atomic<int>* atomic_intermediary_counter = 0;

struct threadInfo {    /* Used as argument to thread_start() */
    int uid;
    JobCard* jobHandle;
};

struct JobCard
{
    JobState jobState;
    const InputVec& inputVec;
    OutputVec& outputVec;
    int activeThreads;
    pthread_t* threadsId;
    const MapReduceClient& client;
    std::atomic<size_t> currentUnmapedPlace;
    std::atomic<size_t> currentUnsortedPlace;
    std::vector<IntermediateVec> intermediateVec;
    sem_t* precentageSem;
    public:
    JobCard(const InputVec& inputVec,OutputVec& outputVec,const MapReduceClient& client,int activeThreads) : 
        inputVec(inputVec),client(client),outputVec(outputVec),activeThreads(activeThreads){
            threadsId = new pthread_t[activeThreads];
            currentUnmapedPlace=0;
            intermediateVec = std::vector<IntermediateVec>(activeThreads);
            sem_init(precentageSem,NULL,1);
    }
    ~JobCard(){
        free(threadsId);
    }

    bool haveMoreToMap(){
        return currentUnmapedPlace < inputVec.size();
    }

    bool haveMoreToSort(){
        return !haveMoreToMap() && currentUnsortedPlace <  inputVec.size();
    }

    size_t getInputIndexToMap(){
        return currentUnmapedPlace.fetch_add(1);
    }

    void updatePrecentage(){
    }
};

void *worker_function(void* arg){
    int uid = ((threadInfo*)arg)->uid;
    JobCard* jobHandle = ((threadInfo*)arg)->jobHandle;

    while (jobHandle->haveMoreToMap())
    {
        size_t place = jobHandle->getInputIndexToMap();
        if(place > jobHandle->inputVec.size()){
            break;
        }
        jobHandle->client.map(jobHandle->inputVec[place].first,jobHandle->inputVec[place].second,&(jobHandle->intermediateVec[uid]));
    }
}


void emit2 (K2* key, V2* value, void* context){
    //The function saves the intermediary element in the context data structures.
    ((IntermediateVec*) context)->push_back(IntermediatePair(key,value));
    // the function updates the number of intermediary elements using atomic counter
    atomic_intermediary_counter++;
}

void emit3 (K3* key, V3* value, void* context){
    //The function saves the output element in the context data structures (output vector)

    //the function updates the number of output elements using atomic counter
}

JobHandle startMapReduceJob(const MapReduceClient& client,
	const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel){
        JobCard* jobHandle = new JobCard(inputVec,outputVec,client,multiThreadLevel);

        for(int i=0;i<multiThreadLevel;i++){
            threadInfo arg = {i,jobHandle};
            pthread_create(&(jobHandle->threadsId[i]),NULL,&worker_function,&arg); //TODO check if succeed
        }
        return jobHandle;
    }

void waitForJob(JobHandle job){

}

void getJobState(JobHandle job, JobState* state){
    *state = ((JobCard*)job)->jobState;
}
void closeJobHandle(JobHandle job);

