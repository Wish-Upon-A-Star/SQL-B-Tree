#ifndef PLATFORM_THREADS_H
#define PLATFORM_THREADS_H

#include <stdint.h>

#if defined(_WIN32)
#include <windows.h>
typedef HANDLE db_thread_t;
typedef CRITICAL_SECTION db_mutex_t;
typedef CONDITION_VARIABLE db_cond_t;
typedef SRWLOCK db_rwlock_t;
typedef DWORD db_thread_return_t;
#define DB_THREAD_CALL WINAPI
static int db_mutex_init(db_mutex_t *mutex) { InitializeCriticalSection(mutex); return 1; }
static void db_mutex_destroy(db_mutex_t *mutex) { DeleteCriticalSection(mutex); }
static void db_mutex_lock(db_mutex_t *mutex) { EnterCriticalSection(mutex); }
static void db_mutex_unlock(db_mutex_t *mutex) { LeaveCriticalSection(mutex); }
static int db_cond_init(db_cond_t *cond) { InitializeConditionVariable(cond); return 1; }
static void db_cond_destroy(db_cond_t *cond) { (void)cond; }
static void db_cond_signal(db_cond_t *cond) { WakeConditionVariable(cond); }
static void db_cond_broadcast(db_cond_t *cond) { WakeAllConditionVariable(cond); }
static void db_cond_wait(db_cond_t *cond, db_mutex_t *mutex) { SleepConditionVariableCS(cond, mutex, INFINITE); }
static int db_cond_timedwait(db_cond_t *cond, db_mutex_t *mutex, uint64_t timeout_ms) { return SleepConditionVariableCS(cond, mutex, (DWORD)timeout_ms) ? 1 : 0; }
static int db_rwlock_init(db_rwlock_t *lock) { InitializeSRWLock(lock); return 1; }
static void db_rwlock_destroy(db_rwlock_t *lock) { (void)lock; }
static void db_rwlock_rdlock(db_rwlock_t *lock) { AcquireSRWLockShared(lock); }
static void db_rwlock_wrlock(db_rwlock_t *lock) { AcquireSRWLockExclusive(lock); }
static void db_rwlock_rdunlock(db_rwlock_t *lock) { ReleaseSRWLockShared(lock); }
static void db_rwlock_wrunlock(db_rwlock_t *lock) { ReleaseSRWLockExclusive(lock); }
#else
#include <pthread.h>
#include <time.h>
typedef pthread_t db_thread_t;
typedef pthread_mutex_t db_mutex_t;
typedef pthread_cond_t db_cond_t;
typedef pthread_rwlock_t db_rwlock_t;
typedef void *db_thread_return_t;
#define DB_THREAD_CALL
static int db_mutex_init(db_mutex_t *mutex) { return pthread_mutex_init(mutex, NULL) == 0; }
static void db_mutex_destroy(db_mutex_t *mutex) { pthread_mutex_destroy(mutex); }
static void db_mutex_lock(db_mutex_t *mutex) { pthread_mutex_lock(mutex); }
static void db_mutex_unlock(db_mutex_t *mutex) { pthread_mutex_unlock(mutex); }
static int db_cond_init(db_cond_t *cond) { return pthread_cond_init(cond, NULL) == 0; }
static void db_cond_destroy(db_cond_t *cond) { pthread_cond_destroy(cond); }
static void db_cond_signal(db_cond_t *cond) { pthread_cond_signal(cond); }
static void db_cond_broadcast(db_cond_t *cond) { pthread_cond_broadcast(cond); }
static void db_cond_wait(db_cond_t *cond, db_mutex_t *mutex) { pthread_cond_wait(cond, mutex); }
static int db_cond_timedwait(db_cond_t *cond, db_mutex_t *mutex, uint64_t timeout_ms) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += (time_t)(timeout_ms / 1000ULL);
    ts.tv_nsec += (long)((timeout_ms % 1000ULL) * 1000000ULL);
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000L;
    }
    return pthread_cond_timedwait(cond, mutex, &ts) == 0;
}
static int db_rwlock_init(db_rwlock_t *lock) { return pthread_rwlock_init(lock, NULL) == 0; }
static void db_rwlock_destroy(db_rwlock_t *lock) { pthread_rwlock_destroy(lock); }
static void db_rwlock_rdlock(db_rwlock_t *lock) { pthread_rwlock_rdlock(lock); }
static void db_rwlock_wrlock(db_rwlock_t *lock) { pthread_rwlock_wrlock(lock); }
static void db_rwlock_rdunlock(db_rwlock_t *lock) { pthread_rwlock_unlock(lock); }
static void db_rwlock_wrunlock(db_rwlock_t *lock) { pthread_rwlock_unlock(lock); }
#endif

typedef db_thread_return_t (DB_THREAD_CALL *db_thread_fn)(void *);

static int db_thread_create(db_thread_t *thread, db_thread_fn fn, void *arg) {
#if defined(_WIN32)
    *thread = CreateThread(NULL, 0, fn, arg, 0, NULL);
    return *thread != NULL;
#else
    return pthread_create(thread, NULL, fn, arg) == 0;
#endif
}

static void db_thread_join(db_thread_t thread) {
#if defined(_WIN32)
    WaitForSingleObject(thread, INFINITE);
    CloseHandle(thread);
#else
    pthread_join(thread, NULL);
#endif
}

#endif
