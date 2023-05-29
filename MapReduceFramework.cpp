#include "MapReduceFramework.h"
#include <math.h>
#include "Barrier.h"
#include <atomic>
#include <algorithm>
#include <semaphore.h>
#include <unordered_map>
#include <iostream>
#include <pthread.h>

sem_t emit3Sem;
std::vector<sem_t*> jobs_sem();

void haltError(std::string text){
    std::cout << "system error: "<< text << std::endl;
    exit(1);
}

struct JobCard
{
    JobState jobState;
    const InputVec& inputVec;
    OutputVec& outputVec;
    pthread_t* threadsId;
    const MapReduceClient& client;
    unsigned int currentUnmapedPlace;
    std::vector<IntermediateVec> intermediateVec;
    sem_t precentageSem,shuffleSem,finishMapSem,reduceSem,mapSem;
    pthread_mutex_t waitForMe;
    std::atomic<size_t> numOfVectorsAfterShuffle;
    int activeThreads,mapCounter,reduceCounter,handledPairs,totalPairsToHandle;
    Barrier barrier;
    public:
    JobCard() = delete;
    JobCard(const InputVec& inputVec,OutputVec& outputVec,const MapReduceClient& client,int activeThreads) : 
        jobState({MAP_STAGE,0}),inputVec(inputVec),outputVec(outputVec),client(client),activeThreads(activeThreads),
        mapCounter(0),reduceCounter(0),handledPairs(0),totalPairsToHandle(0),barrier(activeThreads){
            threadsId = new pthread_t[activeThreads];
            currentUnmapedPlace=0;
            intermediateVec = std::vector<IntermediateVec>(activeThreads);
            if(sem_init(&precentageSem,0,1)!= 0){
                haltError("Faild in sem_init");
            }
            if(sem_init(&shuffleSem,0,1)!=0){
                haltError("Faild in sem_init");
            }
            if(sem_init(&finishMapSem,0,0)!=0){
                haltError("Faild in sem_init");
            }
            if(sem_init(&reduceSem,0,1)!=0){
                haltError("Faild in sem_init");
            }
            if(sem_init(&mapSem,0,1)!=0){
                haltError("Faild in sem_init");
            }
            if(pthread_mutex_init(&waitForMe,0)!=0){
                haltError("Faild in sem_init");
            }
            numOfVectorsAfterShuffle = 0;
    }

    ~JobCard(){
        free(threadsId);
        sem_destroy(&precentageSem);
        sem_destroy(&shuffleSem);
        sem_destroy(&finishMapSem);
        sem_destroy(&mapSem);
        sem_destroy(&reduceSem);
        pthread_mutex_unlock(&waitForMe);
        pthread_mutex_destroy(&waitForMe);
    }

    /**
     * @brief The function return one input pair
     * @return pointer to the input pair
    */
    const InputPair* getInputPairToMap(){
        sem_wait(&mapSem);
        if(inputVec.size() <= currentUnmapedPlace){
            sem_post(&mapSem);
            return NULL;
        }
        const InputPair& tmp = inputVec[currentUnmapedPlace];
        currentUnmapedPlace++;
        sem_post(&mapSem);
        return &tmp;
    }

    /**
     * @brief The function return one vector to reduce
    */
    IntermediateVec getVectorToReduce(){
        sem_wait(&reduceSem);
        if(intermediateVec.empty()){
            sem_post(&reduceSem);
            return IntermediateVec();
        }
        IntermediateVec tmp = intermediateVec.back();
        intermediateVec.pop_back();
        sem_post(&reduceSem);
        return tmp;
    }

    void updatePrecentage(stage_t stage){
        sem_wait(&precentageSem);
        switch(stage){
            case MAP_STAGE:
                mapCounter++;
                jobState.percentage = (mapCounter*100.0)/inputVec.size();
                if(jobState.percentage >= 100 ){
                    jobState.stage = SHUFFLE_STAGE;
                    jobState.percentage = 0;
                    sem_post(&finishMapSem);
                }
            break;
            case SHUFFLE_STAGE:
                jobState.percentage = (handledPairs*100.0)/totalPairsToHandle;
                if(jobState.percentage >= 100){
                    jobState.percentage = 0;
                    jobState.stage = REDUCE_STAGE;
                }
            break;
            case REDUCE_STAGE:
                reduceCounter++;
                jobState.percentage = ((reduceCounter) * 100)/numOfVectorsAfterShuffle;
            break;
            default:
            break;
        }
        sem_post(&precentageSem);
    }

    K2* getMaxKey(std::vector<IntermediateVec>& unShuffledVectors){
        K2* highestKey = NULL;
        for(IntermediateVec& vec: unShuffledVectors){
            if(!vec.empty()){
                if(highestKey==NULL){
                    highestKey = vec.back().first;
                }
                if(*highestKey < *(vec.back().first)){
                    highestKey = vec.back().first;
                }
            }
        }
        return highestKey;
    }

    int countPairs(std::vector<IntermediateVec>& vecOfVecs){
        int counter = 0;
        for(IntermediateVec& vec : vecOfVecs){
            counter+=vec.size();
        }
        return counter;
    }


    void readyToShuffle(int uid){
        if(uid == 0){
            sem_wait(&finishMapSem);
            std::vector<IntermediateVec> unShuffledVectors = intermediateVec;
            totalPairsToHandle = countPairs(unShuffledVectors);
            intermediateVec.clear();
            numOfVectorsAfterShuffle=0;
            K2* highestKey;
            while(!unShuffledVectors.empty()){
                highestKey = getMaxKey(unShuffledVectors);
                if(highestKey==NULL){
                    break;
                }
                numOfVectorsAfterShuffle++;
                intermediateVec.push_back(IntermediateVec());
                for(std::vector<IntermediateVec>::iterator it = unShuffledVectors.begin();it != unShuffledVectors.end();++it){
                    while (!it->empty() && !(*(it->back().first) < *highestKey ||  *highestKey < *(it->back().first)))
                    {
                        intermediateVec.back().push_back(it->back());
                        it->pop_back();
                    }
                    if(it->empty()){
                        unShuffledVectors.erase(it);
                        --it;
                    }
                }
                handledPairs = countPairs(intermediateVec);
                updatePrecentage(SHUFFLE_STAGE);
            }
        }
        barrier.barrier();
    }

};

struct threadInfo {    /* Used as argument to thread_start() */
    int uid;
    JobCard* jobHandle;
    threadInfo(int uid,JobCard* jobHandle): uid(uid),jobHandle(jobHandle){};

};
void* worker_function(void* arg){
    int uid = ((threadInfo*)arg)->uid;
    JobCard* jobHandle = ((threadInfo*)arg)->jobHandle;
    free(arg);
    const InputPair* input = jobHandle->getInputPairToMap();
    while (input != nullptr)
    {
        jobHandle->client.map(input->first,input->second,&(jobHandle->intermediateVec[uid]));
        jobHandle->updatePrecentage(MAP_STAGE);
        input = jobHandle->getInputPairToMap();
    }
    std::sort(jobHandle->intermediateVec[uid].begin(),jobHandle->intermediateVec[uid].end(),[](const IntermediatePair& left,const IntermediatePair& right){
        return *left.first < *right.first;
    });

    jobHandle->readyToShuffle(uid);

    IntermediateVec vectorToReduce = jobHandle->getVectorToReduce();
    while (!vectorToReduce.empty())
    {
        jobHandle->client.reduce(&vectorToReduce,&(jobHandle->outputVec));
        jobHandle->updatePrecentage(REDUCE_STAGE);
        vectorToReduce = jobHandle->getVectorToReduce();
    }
    return nullptr;
}

    


void emit2 (K2* key, V2* value, void* context){
    ((IntermediateVec*) context)->push_back(IntermediatePair(key,value));
}

void emit3 (K3* key, V3* value, void* context){
    //The function saves the output element in the context data structures (output vector)
    sem_wait(&emit3Sem);
    ((OutputVec*)context)->push_back(OutputPair(key,value));
    sem_post(&emit3Sem);
    //the function updates the number of output elements using atomic counter
}


JobHandle startMapReduceJob(const MapReduceClient& client,
	const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel){
        sem_init(&emit3Sem,0,1);
        
        JobCard* jobHandle = new JobCard(inputVec,outputVec,client,multiThreadLevel);
        for(int i=0;i<multiThreadLevel;i++){
            threadInfo* arg = new threadInfo(i,jobHandle);
            if(pthread_create(&(jobHandle->threadsId[i]),NULL,&worker_function,arg)==-1){
                haltError("Fail to create a new Thread");
            }
        }
        return jobHandle;
    }

void waitForJob(JobHandle job){
    if(job != NULL){
        JobCard* jobPtr = (JobCard*)job;
        pthread_mutex_lock(&(jobPtr->waitForMe));
        for(int i=0;i < jobPtr->activeThreads;i++){
            pthread_join(jobPtr->threadsId[i],nullptr);
        }
        if(job != NULL){
            pthread_mutex_unlock(&(jobPtr->waitForMe));
        }
    }
}

void getJobState(JobHandle job, JobState* state){
    *state = ((JobCard*)job)->jobState;
}

void closeJobHandle(JobHandle job){
    JobCard* jobPtr = (JobCard*)job;
    if(!(jobPtr->jobState.stage == REDUCE_STAGE && jobPtr->jobState.percentage >=100)){
        waitForJob(job);
    }
    if(jobPtr->jobState.stage == REDUCE_STAGE && jobPtr->jobState.percentage >=100){
        delete jobPtr;
        jobPtr =NULL;
    }
}



