#include <bits/stdc++.h>

struct semaphore {
  int count;
  queue<int> processQueue;
}


void wait(semaphore* sem) {
  
  sem->count--; //decrement the count
  
  if(sem->count < 0) { //if sem count < 0, add the process to wait queue
    sem->processQueue.push(curPid);

    //call to reschedule
  }
  
  return;
}


void signal(semaphore* sem) {
  
  sem->count++; //increment the count
  
  if(!sem->processQueue.empty()) { //if sem count <= 0, some process is waiting
    int pid = sem->processQueue.front();
    sem->processQueue.pop()
   
    readyQueue.push(pid); //make the process ready so that it can acquire the semaphore when it becomes current
  }
  
  return;
}
