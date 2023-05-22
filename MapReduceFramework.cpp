#include "MapReduceFramework.h"

std::atomic<int>* atomic_intermediary_counter = 0;
sem_t *emit3Sem;


struct JobCard
{
    JobState jobState;s
    const InputVec* inputVec;
    OutputVec* outputVec;
    int activeThreads;
    pthread_t* threadsId;
    const MapReduceClient* client;
    unsigned int currentUnmapedPlace;
    std::vector<IntermediateVec> intermediateVec;
    sem_t precentageSem,shuffleSem,finishMapSem,reduceSem,mapSem;
    std::atomic<size_t> numOfVectorsAfterShuffle;
    Barrier barrier;
    public:
    JobCard() = delete;
    JobCard(const InputVec* inputVec,OutputVec* outputVec,const MapReduceClient* client,int activeThreads) : 
        jobState(),inputVec(inputVec),outputVec(outputVec),client(client),activeThreads(activeThreads),barrier(activeThreads){
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



    const InputPair& getInputPairToMap(){
        std::cout << "hello son of a bitch!!" << std::endl;
        if(inputVec->size() <= currentUnmapedPlace){
            return {nullptr,nullptr};
        }
        sem_wait(&mapSem);
        const InputPair& tmp = (*inputVec)[currentUnmapedPlace];
        currentUnmapedPlace++;
        sem_post(&mapSem);
        return tmp;
    }

    IntermediateVec getVectorToReduce(){
        if(intermediateVec.empty()){
            return IntermediateVec();
        }
        sem_wait(&reduceSem);
        IntermediateVec& tmp = intermediateVec.back();
        intermediateVec.pop_back();
        sem_post(&reduceSem);
        return tmp;
    }

    void updatePrecentage(stage_t stage){
        sem_wait(&precentageSem);
        switch(stage){
            case MAP_STAGE:
                jobState.percentage += 1.0/(*inputVec).size();
                if(jobState.percentage >= 1 ){
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
                jobState.percentage += 1.0/numOfVectorsAfterShuffle;
                if(jobState.percentage >= 1){
                    jobState.stage = UNDEFINED_STAGE;
                    jobState.percentage = 0;
                }
            break;
            default:
            break;
        }
        sem_post(&precentageSem);
    }


    void readyToShuffle(int uid){
        if(uid == 0){
            sem_wait(&finishMapSem);
            std::vector<IntermediateVec> shuffled;
            while(!intermediateVec.empty()){
                K2* tmp;
                for(IntermediateVec& vec :intermediateVec){
                    if(tmp ==NULL){
                        tmp = vec.back().first;
                    }
                    else if (tmp < vec.back().first)
                    {
                        tmp = vec.back().first;
                    }
                }
                numOfVectorsAfterShuffle++;
                IntermediateVec subIntermediateVec;
                for(IntermediateVec& vec :intermediateVec){
                   while(!(vec.back().first < tmp ||  tmp < vec.back().first)){
                    subIntermediateVec.push_back(vec.back());
                    vec.pop_back();
                   }
                }
                shuffled.push_back(subIntermediateVec);
            }
            intermediateVec = shuffled;
        }
        barrier.barrier();
    }

};

struct threadInfo {    /* Used as argument to thread_start() */
    int uid;
    JobCard* jobHandle;
};
void* worker_function(void* arg){
    int uid = ((threadInfo*)arg)->uid;
    JobCard* jobHandle = ((threadInfo*)arg)->jobHandle;
    const InputPair& input = jobHandle->getInputPairToMap();
    while (input.first != nullptr)
    {
        jobHandle->client->map(input.first,input.second,&(jobHandle->intermediateVec[uid]));
        jobHandle->updatePrecentage(MAP_STAGE);
        const InputPair& input = jobHandle->getInputPairToMap();
    }
    std::sort(jobHandle->intermediateVec[uid].begin(),jobHandle->intermediateVec[uid].end(),[](const IntermediatePair& left,const IntermediatePair& right){
        return left.first < right.first;
    });

    jobHandle->readyToShuffle(uid);

    IntermediateVec vectorToReduce = jobHandle->getVectorToReduce();
    while (!vectorToReduce.empty())
    {
        jobHandle->client->reduce(&vectorToReduce,jobHandle->outputVec);
        jobHandle->updatePrecentage(REDUCE_STAGE);
        IntermediateVec vectorToReduce = jobHandle->getVectorToReduce();
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
    sem_wait(emit3Sem);
    ((OutputVec*)context)->push_back(OutputPair(key,value));
    sem_post(emit3Sem);
    //the function updates the number of output elements using atomic counter
}

JobHandle startMapReduceJob(const MapReduceClient& client,
	const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel){
        if(emit3Sem != NULL){
            sem_init(emit3Sem,0,1);
        }
        
        JobCard* jobHandle = new JobCard(&inputVec,&outputVec,&client,multiThreadLevel);
        for(int i=0;i<multiThreadLevel;i++){
            threadInfo arg = {i,jobHandle};
            pthread_create(&(jobHandle->threadsId[i]),NULL,&worker_function,&arg); //TODO check if succeed
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

