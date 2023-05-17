#include "MapReduceFramework.h";
#include <atomic>;
#include <algorithm>;
#include <semaphore.h>;
#include <unordered_map>;
#include <iostream>;

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
    sem_t* precentageSem,*shuffleSem,*finishMapSem;
    std::atomic<size_t> numOfVectorsAfterShuffle;
    bool closeOnFinish;
    Barrier barrier;
    public:
    JobCard(const InputVec& inputVec,OutputVec& outputVec,const MapReduceClient& client,int activeThreads) : 
        inputVec(inputVec),client(client),outputVec(outputVec),activeThreads(activeThreads),barrier(activeThreads){
            threadsId = new pthread_t[activeThreads];
            currentUnmapedPlace=0;
            intermediateVec = std::vector<IntermediateVec>(activeThreads);
            sem_init(precentageSem,NULL,1);
            sem_init(shuffleSem,NULL,1);
            sem_init(finishMapSem,NULL,0);
            numOfVectorsAfterShuffle = 0;
            closeOnFinish= false;
    }

    ~JobCard(){
        free(threadsId);
        sem_destroy(precentageSem);
        sem_destroy(shuffleSem);
        sem_destroy(finishMapSem);
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

    void updatePrecentage(stage_t stage){
        sem_close(precentageSem);
        switch(stage){
            case MAP_STAGE:
                jobState.percentage += 1.0/inputVec.size();
                if(jobState.percentage >= 1 ){
                jobState.stage = SHUFFLE_STAGE;
                jobState.percentage = 0;
                sem_post(finishMapSem);
                }
            break;
            case SHUFFLE_STAGE:
                jobState.stage = REDUCE_STAGE;
                jobState.percentage = 0;
            break;
            case REDUCE_STAGE:
                jobState.percentage += 1.0/inputVec.size();
                if(jobState.percentage >= 1){
                    jobState.stage = UNDEFINED_STAGE;
                    jobState.percentage = 0;
                    if(closeOnFinish){
                        //TODO close the job
                    }
                }
            break;
        }
        sem_post(precentageSem);
    }

    void closeOnFinish(){
        if(jobState.stage == UNDEFINED_STAGE){
            //TODO close the job
        }
        closeOnFinish = true;
    }

    void readyToShuffle(int uid){
        if(uid == 0){
            sem_close(finishMapSem);
            std::vector<IntermediateVec> shuffled;
            while(!intermediateVec.empty()){
                std::vector<K2*> tmp;
                for(IntermediateVec& vec :intermediateVec){
                    tmp.push_back(vec.back().first);
                }
                K2* highest = std::max(tmp.begin(),tmp.end());
            }
        }
        barrier.barrier();
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
        jobHandle->updatePrecentage(MAP_STAGE);
    }
    std::sort(jobHandle->intermediateVec[uid].begin(),jobHandle->intermediateVec[uid].end(),[](const auto& left,const auto& right){
        return left->first < right->first;
    })

    jobHandle->readyToShuffle(uid);
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
void closeJobHandle(JobHandle job){
    ((JobCard*)job)->closeOnFinish()
}

