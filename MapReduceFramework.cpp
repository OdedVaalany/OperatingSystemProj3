#include "MapReduceFramework.h"
#include <math.h>
std::atomic<int>* atomic_intermediary_counter = 0;
sem_t emit3Sem;


struct JobCard
{
    JobState jobState;
    const InputVec& inputVec;
    OutputVec& outputVec;
    int activeThreads;
    pthread_t* threadsId;
    const MapReduceClient& client;
    unsigned int currentUnmapedPlace;
    std::vector<IntermediateVec> intermediateVec;
    sem_t precentageSem,shuffleSem,finishMapSem,reduceSem,mapSem;
    std::atomic<size_t> numOfVectorsAfterShuffle;
    Barrier barrier;
    public:
    JobCard() = delete;
    JobCard(const InputVec& inputVec,OutputVec& outputVec,const MapReduceClient& client,int activeThreads) : 
        jobState({MAP_STAGE,0}),inputVec(inputVec),outputVec(outputVec),client(client),activeThreads(activeThreads),barrier(activeThreads){
            threadsId = new pthread_t[activeThreads];
            currentUnmapedPlace=0;
            intermediateVec = std::vector<IntermediateVec>(activeThreads);
            sem_init(&precentageSem,0,1);
            sem_init(&shuffleSem,0,1);
            sem_init(&finishMapSem,0,0);
            sem_init(&reduceSem,0,1);
            sem_init(&mapSem,0,1);
            numOfVectorsAfterShuffle = 0;
    }

    ~JobCard(){
        free(threadsId);
        sem_destroy(&precentageSem);
        sem_destroy(&shuffleSem);
        sem_destroy(&finishMapSem);
        sem_destroy(&mapSem);
        sem_destroy(&reduceSem);
    }



    const InputPair* getInputPairToMap(){
        sem_wait(&mapSem);
        if(inputVec.size() <= currentUnmapedPlace){
            return NULL;
        }
        const InputPair& tmp = inputVec[currentUnmapedPlace];
        currentUnmapedPlace++;
        sem_post(&mapSem);
        return &tmp;
    }

    IntermediateVec getVectorToReduce(){
        if(intermediateVec.empty()){
            return IntermediateVec();
        }
        sem_wait(&reduceSem);
        IntermediateVec tmp = intermediateVec.back();
        intermediateVec.pop_back();
        sem_post(&reduceSem);
        return tmp;
    }

    void updatePrecentage(stage_t stage){
        sem_wait(&precentageSem);
        switch(stage){
            case MAP_STAGE:
                jobState.percentage = (currentUnmapedPlace*100.0)/inputVec.size();
                if(jobState.percentage >= 100 ){
                    jobState.stage = SHUFFLE_STAGE;
                    jobState.percentage = 0;
                    sem_post(&finishMapSem);
                }
            break;
            case SHUFFLE_STAGE:
                jobState.stage = REDUCE_STAGE;
                jobState.percentage = 0;
            break;
            case REDUCE_STAGE:
                jobState.percentage = ((numOfVectorsAfterShuffle -intermediateVec.size()) * 100)/numOfVectorsAfterShuffle;
            break;
            default:
            break;
        }
        sem_post(&precentageSem);
    }


    void readyToShuffle(int uid){
        if(uid == 0){
            sem_wait(&finishMapSem);
            std::vector<IntermediateVec> unShuffledVectors = intermediateVec;
            intermediateVec.clear();
            numOfVectorsAfterShuffle=0;
            K2* highestKey;
            while(!unShuffledVectors.empty()){
                highestKey = NULL;
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
        return left.first < right.first;
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
    //The function saves the intermediary element in the context data structures.
    ((IntermediateVec*) context)->push_back(IntermediatePair(key,value));
    // the function updates the number of intermediary elements using atomic counter
    atomic_intermediary_counter++;
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
                std::cout << "fail" << std::endl;
            }
        }
        return jobHandle;
    }

void waitForJob(JobHandle job){
    if(job != NULL){
        pthread_join(((JobCard*)job)->threadsId[0],nullptr);
    }
}

void getJobState(JobHandle job, JobState* state){
    *state = ((JobCard*)job)->jobState;
}
void closeJobHandle(JobHandle job){
}

