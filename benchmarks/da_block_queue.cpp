
#include "da_block_queue.h"


void DABlockQueue::LockQueue()  //queue lock
{
    pthread_mutex_lock(&mutex);
}
void DABlockQueue::UnlockQueue()  //队列解锁
{
    pthread_mutex_unlock(&mutex);
}
void DABlockQueue::ProductWait()  //队列满，生产者等待
{
    pthread_cond_wait(&full,&mutex);
}
void DABlockQueue::ConsumeWait()  //队列空，消费者等待
{
    pthread_cond_wait(&empty,&mutex);
}
void DABlockQueue::NotifyProduct()  //队列不为满时，通知生产者
{
    pthread_cond_signal(&full);
}
void DABlockQueue::NotifyConsume()  //队列不为空时，通知消费者
{
    pthread_cond_signal(&empty);
}
bool DABlockQueue::IsEmpty()
{
    return (q.size() == 0 ? true : false);
}
bool DABlockQueue::IsFull()
{
    return (q.size() == cap ? true : false);
}

DABlockQueue::DABlockQueue(size_t _cap = 50):cap(_cap) //构造函数
{
    pthread_mutex_init(&mutex,NULL);
    pthread_cond_init(&full,NULL);
    pthread_cond_init(&empty,NULL);
}
DABlockQueue::~DABlockQueue()
{
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&full);
    pthread_cond_destroy(&empty);
}
void DABlockQueue::push_data(BaseQuery* data)
{
    LockQueue();
    while(IsFull()) //队列满
    {
        NotifyConsume();
        std::cout<<"queue full,notify consume data,product stop!!"<<std::endl;
        ProductWait();
    }
    //队列不满,生产者插入数据，通知消费者队列中已经有数据了
    q.push_back(data);
    NotifyConsume();
    UnlockQueue();
}
BaseQuery* DABlockQueue::pop_data()
{
    BaseQuery* data;
    LockQueue();
    while(IsEmpty())  //队列为空
    {
        NotifyProduct();
        std::cout<<"queue empty,notify product data,consume stop!!"<<std::endl;
        list<BaseQuery*>().swap(q);
        ConsumeWait();
    }
    //队列不为空
    data = q.front();
    q.pop_front();
    NotifyProduct();
    UnlockQueue();
    return data;
}

