// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/util/thread-pool.h"
#include "arrow/util/io-util.h"
#include "arrow/util/logging.h"

#include <algorithm>
#include <string>

namespace arrow {
namespace internal {

class ThreadPoolLock {
public:
  ThreadPoolLock(pthread_mutex_t* mutex) : mutex_(mutex) {
    lock();
  };
  ~ThreadPoolLock() {
    unlock();
  }
  void lock() {
    pthread_mutex_lock(mutex_);
  }
  void unlock() {
    pthread_mutex_unlock(mutex_);
  }

private:
  pthread_mutex_t* mutex_;
};

class ThreadPool::Thread {
public:
  Thread() : thread_(new pthread_t()), function_(new std::function<void()>()) {}
  void Start(std::shared_ptr<State> state, std::list<Thread>::iterator it) {
    *function_ = [state, it] { WorkerLoop(state, it); };
    auto thunk = [](void* context) -> void* {
      (*static_cast<std::function<void()>*>(context))();
      return NULL;
    };
    pthread_create(it->thread(), NULL, thunk, function_);
  }
  pthread_t *thread() {
    return thread_;
  }
  ~Thread() {
    delete function_;
    delete thread_;
  }
private:
  pthread_t* thread_;
  std::function<void()>* function_;
};

struct ThreadPool::State {
  State() : desired_capacity_(0), please_shutdown_(false), quick_shutdown_(false) {}

  pthread_mutex_t mutex_;
  pthread_cond_t cv_;
  pthread_cond_t cv_shutdown_;

  std::list<Thread> workers_;
  // Trashcan for finished threads
  std::vector<Thread> finished_workers_;
  std::deque<std::function<void()>> pending_tasks_;

  // Desired number of threads
  int desired_capacity_;
  // Are we shutting down?
  bool please_shutdown_;
  bool quick_shutdown_;
};

ThreadPool::ThreadPool()
    : sp_state_(std::make_shared<ThreadPool::State>()),
      state_(sp_state_.get()),
      shutdown_on_destroy_(true) {}

ThreadPool::~ThreadPool() {
  if (shutdown_on_destroy_) {
    ARROW_UNUSED(Shutdown(false /* wait */));
  }
}

Status ThreadPool::SetCapacity(int threads) {
  ThreadPoolLock lock(&state_->mutex_);
  if (state_->please_shutdown_) {
    return Status::Invalid("operation forbidden during or after shutdown");
  }
  if (threads <= 0) {
    return Status::Invalid("ThreadPool capacity must be > 0");
  }
  CollectFinishedWorkersUnlocked();

  state_->desired_capacity_ = threads;
  int diff = static_cast<int>(threads - state_->workers_.size());
  if (diff > 0) {
    LaunchWorkersUnlocked(diff);
  } else if (diff < 0) {
    // Wake threads to ask them to stop
    DCHECK(pthread_cond_broadcast(&state_->cv_));
  }
  return Status::OK();
}

int ThreadPool::GetCapacity() {
  ThreadPoolLock lock(&state_->mutex_);
  return state_->desired_capacity_;
}

int ThreadPool::GetActualCapacity() {
  ThreadPoolLock lock(&state_->mutex_);
  return static_cast<int>(state_->workers_.size());
}

Status ThreadPool::Shutdown(bool wait) {
  ThreadPoolLock lock(&state_->mutex_);

  if (state_->please_shutdown_) {
    return Status::Invalid("Shutdown() already called");
  }
  state_->please_shutdown_ = true;
  state_->quick_shutdown_ = !wait;
  DCHECK(pthread_cond_broadcast(&state_->cv_));
  while (state_->workers_.empty()) {
    pthread_cond_wait(&state_->cv_shutdown_, &state_->mutex_);
  }
  if (!state_->quick_shutdown_) {
    DCHECK_EQ(state_->pending_tasks_.size(), 0);
  } else {
    state_->pending_tasks_.clear();
  }
  CollectFinishedWorkersUnlocked();
  return Status::OK();
}

void ThreadPool::CollectFinishedWorkersUnlocked() {
  for (auto thread : state_->finished_workers_) {
    // Make sure OS thread has exited
    pthread_join(*thread.thread(), NULL);
  }
  state_->finished_workers_.clear();
}

void ThreadPool::LaunchWorkersUnlocked(int threads) {
  std::shared_ptr<State> state = sp_state_;

  for (int i = 0; i < threads; i++) {
    state_->workers_.emplace_back();
    auto it = --(state_->workers_.end());
    it->Start(state, it);
  }
}

void ThreadPool::WorkerLoop(std::shared_ptr<State> state,
                            std::list<Thread>::iterator it) {
  ThreadPoolLock lock(&state->mutex_);

  // Since we hold the lock, `it` now points to the correct thread object
  // (LaunchWorkersUnlocked has exited)
  // XXX DCHECK_EQ(std::this_thread::get_id(), it->get_id());

  // If too many threads, we should secede from the pool
  const auto should_secede = [&]() -> bool {
    return state->workers_.size() > static_cast<size_t>(state->desired_capacity_);
  };

  while (true) {
    // By the time this thread is started, some tasks may have been pushed
    // or shutdown could even have been requested.  So we only wait on the
    // condition variable at the end of the loop.

    // Execute pending tasks if any
    while (!state->pending_tasks_.empty() && !state->quick_shutdown_) {
      // We check this opportunistically at each loop iteration since
      // it releases the lock below.
      if (should_secede()) {
        break;
      }
      {
        std::function<void()> task = std::move(state->pending_tasks_.front());
        state->pending_tasks_.pop_front();
        lock.unlock();
        task();
      }
      lock.lock();
    }
    // Now either the queue is empty *or* a quick shutdown was requested
    if (state->please_shutdown_ || should_secede()) {
      break;
    }
    // Wait for next wakeup
    pthread_cond_wait(&state->cv_, &state->mutex_);
  }

  // We're done.  Move our thread object to the trashcan of finished
  // workers.  This has two motivations:
  // 1) the thread object doesn't get destroyed before this function finishes
  //    (but we could call thread::detach() instead)
  // 2) we can explicitly join() the trashcan threads to make sure all OS threads
  //    are exited before the ThreadPool is destroyed.  Otherwise subtle
  //    timing conditions can lead to false positives with Valgrind.
  // XXX DCHECK_EQ(std::this_thread::get_id(), it->get_id());
  state->finished_workers_.push_back(std::move(*it));
  state->workers_.erase(it);
  if (state->please_shutdown_) {
    // Notify the function waiting in Shutdown().
    DCHECK(pthread_cond_signal(&state->cv_shutdown_));
  }
}

Status ThreadPool::SpawnReal(std::function<void()> task) {
  {
    ThreadPoolLock lock(&state_->mutex_);
    if (state_->please_shutdown_) {
      return Status::Invalid("operation forbidden during or after shutdown");
    }
    CollectFinishedWorkersUnlocked();
    state_->pending_tasks_.push_back(std::move(task));
  }
  DCHECK(pthread_cond_signal(&state_->cv_));
  return Status::OK();
}

Status ThreadPool::Make(int threads, std::shared_ptr<ThreadPool>* out) {
  auto pool = std::shared_ptr<ThreadPool>(new ThreadPool());
  RETURN_NOT_OK(pool->SetCapacity(threads));
  *out = std::move(pool);
  return Status::OK();
}

// ----------------------------------------------------------------------
// Global thread pool

static int ParseOMPEnvVar(const char* name) {
  // OMP_NUM_THREADS is a comma-separated list of positive integers.
  // We are only interested in the first (top-level) number.
  std::string str;
  if (!GetEnvVar(name, &str).ok()) {
    return 0;
  }
  auto first_comma = str.find_first_of(',');
  if (first_comma != std::string::npos) {
    str = str.substr(0, first_comma);
  }
  try {
    return std::max(0, std::stoi(str));
  } catch (...) {
    return 0;
  }
}

int ThreadPool::DefaultCapacity() {
  int capacity, limit;
  capacity = ParseOMPEnvVar("OMP_NUM_THREADS");
  if (capacity == 0) {
    capacity = std::thread::hardware_concurrency();
  }
  limit = ParseOMPEnvVar("OMP_THREAD_LIMIT");
  if (limit > 0) {
    capacity = std::min(limit, capacity);
  }
  if (capacity == 0) {
    ARROW_LOG(WARNING) << "Failed to determine the number of available threads, "
                          "using a hardcoded arbitrary value";
    capacity = 4;
  }
  return capacity;
}

// Helper for the singleton pattern
std::shared_ptr<ThreadPool> ThreadPool::MakeCpuThreadPool() {
  std::shared_ptr<ThreadPool> pool;
  DCHECK_OK(ThreadPool::Make(ThreadPool::DefaultCapacity(), &pool));
  // On Windows, the global ThreadPool destructor may be called after
  // non-main threads have been killed by the OS, and hang in a condition
  // variable.
  // On Unix, we want to avoid leak reports by Valgrind.
#ifdef _WIN32
  pool->shutdown_on_destroy_ = false;
#endif
  return pool;
}

ThreadPool* GetCpuThreadPool() {
  static std::shared_ptr<ThreadPool> singleton = ThreadPool::MakeCpuThreadPool();
  return singleton.get();
}

}  // namespace internal

int GetCpuThreadPoolCapacity() { return internal::GetCpuThreadPool()->GetCapacity(); }

Status SetCpuThreadPoolCapacity(int threads) {
  return internal::GetCpuThreadPool()->SetCapacity(threads);
}

}  // namespace arrow
