{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Data.PowerQueue
    ( -- * Worker descriptions
      QueueWorker, newQueueWorker, JobResult(..)
      -- * Queue control
    , Queue, newQueue, mapQueue, enqueueJob
      -- * (persistent) queue backends
    , QueueBackend(..), mapBackend, basicChanBackend
      -- * execution strategies
    , workStep
      -- ** A local worker
    , LocalWorkerConfig(..), localQueueWorker
    )
where

import Control.Concurrent.Async
import Control.Exception
import Control.Monad
import Data.Bifunctor
import Data.Functor.Contravariant
import Data.Functor.Contravariant.Divisible
import Data.IORef
import qualified Control.Concurrent.Chan as C

-- | Result of the job
data JobResult
    = JOk
      -- ^ job is complete
    | JRetry
      -- ^ job execution should be retried
    deriving (Show, Eq)

data QueueBackend j
    = forall tx m. Monad m =>
    QueueBackend
    { qb_lift :: forall a. m a -> IO a
      -- ^ lift an action from the backend monad into 'IO'
    , qb_enqueue :: j -> m ()
      -- ^ enqueue a job
    , qb_dequeue :: m (tx, j)
      -- ^ dequeue a single job, block if no job available
    , qb_confirm :: tx -> m ()
      -- ^ mark a single job as confirmed
    , qb_rollback :: tx -> m ()
      -- ^ mark a single job as failed
    }

-- | A very basic in memory backend using only data structures from the base library.
-- It should only be used for testing and serves as an implementation example
basicChanBackend :: forall j. IO (QueueBackend j)
basicChanBackend =
    do jobChannel <- C.newChan
       inProgress <- newIORef (0, [])
       let dequeueHandler :: j -> (Int, [(Int, j)]) -> ((Int, [(Int, j)]), (Int, j))
           dequeueHandler job st@(idx, _) =
               let entry = (idx, job)
               in ( bimap (+1) (entry:) st
                  , entry
                  )
           forget txId =
               atomicModifyIORef' inProgress $ \st ->
               ( second (filter (\x -> fst x == txId)) st
               , snd $ second (lookup txId) st
               )
       pure
           QueueBackend
           { qb_lift = id
           , qb_enqueue = C.writeChan jobChannel
           , qb_dequeue =
                   do nextJob <- C.readChan jobChannel
                      atomicModifyIORef' inProgress (dequeueHandler nextJob)
           , qb_confirm = void . forget
           , qb_rollback =
                   \txId ->
                   do oldVal <- forget txId
                      case oldVal of
                        Just j -> C.writeChan jobChannel j
                        Nothing -> pure () -- should not really happen ...
           }

mapBackend :: (a -> b) -> (b -> a) -> QueueBackend a -> QueueBackend b
mapBackend f g (QueueBackend qlift qenq qdeq qconf qroll) =
    QueueBackend
    { qb_lift = qlift
    , qb_enqueue = qenq . g
    , qb_dequeue = fmap (second f) qdeq
    , qb_confirm = qconf
    , qb_rollback = qroll
    }

data QueueWorker j
    = QueueWorker
    { qw_execute :: j -> IO JobResult
      -- ^ run a job
    }

newQueueWorker :: (j -> IO JobResult) -> QueueWorker j
newQueueWorker exec =
    QueueWorker
    { qw_execute = exec
    }

instance Contravariant QueueWorker where
    contramap  f (QueueWorker qexec) =
        QueueWorker
        { qw_execute = \val -> qexec (f val)
        }

instance Divisible QueueWorker where
    divide f (QueueWorker qe1) (QueueWorker qe2) =
        QueueWorker
        { qw_execute = \val ->
                do let (l, r) = f val
                   ok1 <- qe1 l
                   ok2 <- qe2 r
                   pure $
                       if ok1 /= JOk || ok2 /= JOk
                       then JRetry
                       else JOk
        }
    conquer =
        newQueueWorker $ \_ -> pure JOk

data Queue j
    = Queue
    { q_worker :: !(QueueWorker j)
    , q_backend :: !(QueueBackend j)
    }

mapQueue :: (a -> b) -> (b -> a) -> Queue a -> Queue b
mapQueue f g q =
    Queue
    { q_worker = contramap g (q_worker q)
    , q_backend = mapBackend f g (q_backend q)
    }

-- | Create a new queue description
newQueue :: QueueBackend j -> QueueWorker j -> Queue j
newQueue qb qw =
    Queue
    { q_worker = qw
    , q_backend = qb
    }

-- | Add a 'Job' to the 'Queue'
enqueueJob :: j -> Queue j -> IO ()
enqueueJob j q =
    let enqueue QueueBackend{..} = qb_lift (qb_enqueue j)
    in enqueue (q_backend q)

-- | Execute a single work step: attempt a dequeue and run the job. Use
-- to implement a queue worker, such as 'localQueueWorker'
workStep :: Queue j -> IO ()
workStep q = workStepInternal (q_backend q) (q_worker q)

workStepInternal :: QueueBackend j -> QueueWorker j -> IO ()
workStepInternal QueueBackend{..} QueueWorker{..} =
    let acquire = qb_lift qb_dequeue
        onError (txId, _) = qb_lift (qb_rollback txId)
        go (txId, job) =
            do execRes <-
                   try $ qw_execute job
               case execRes of
                 Left (_ :: SomeException) ->
                     qb_lift (qb_rollback txId)
                 Right res ->
                     case res of
                       JOk -> qb_lift (qb_confirm txId)
                       JRetry -> qb_lift (qb_rollback txId)
    in bracketOnError acquire onError go

data LocalWorkerConfig
    = LocalWorkerConfig
    { lwc_concurrentJobs :: !Int
    }

-- | (Concurrently) run pending jobs on local machine in current process
localQueueWorker :: LocalWorkerConfig -> Queue j -> IO ()
localQueueWorker cfg q =
    replicateConcurrently_ (lwc_concurrentJobs cfg) $
    forever $ workStep q
