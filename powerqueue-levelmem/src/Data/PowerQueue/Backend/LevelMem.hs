{-# LANGUAGE RecordWildCards #-}
module Data.PowerQueue.Backend.LevelMem
    ( newLevelMemBackend
    )
where

import Control.Concurrent.STM
import Data.PowerQueue
import qualified Control.Concurrent.Chan.Unagi.Bounded as C
import qualified STMContainers.Map as SMap
import qualified STMContainers.Set as SSet

type Chan a = (C.InChan a, C.OutChan a)
type JobIdx = Int

data LevelMem j
    = LevelMem
    { lm_queueKey :: !(TVar JobIdx)
    , lm_jobs :: !(SMap.Map JobIdx j)
    , lm_queue :: !(Chan JobIdx)
    , lm_inProgress :: !(SSet.Set JobIdx)
    }

newLevelMemBackend :: LevelMem j -> QueueBackend j
newLevelMemBackend levelMem =
    QueueBackend
    { qb_lift = id
    , qb_enqueue = flip enqueue levelMem
    , qb_dequeue = dequeue levelMem
    , qb_confirm = flip ack levelMem
    , qb_rollback = flip nack levelMem
    }

enqueue :: j -> LevelMem j -> IO ()
enqueue job LevelMem{..} =
    do ix <-
           atomically $
           do key <- readTVar lm_queueKey
              SMap.insert job key lm_jobs
              writeTVar lm_queueKey (key+1)
              return key
       C.writeChan (fst lm_queue) ix

dequeue :: LevelMem j -> IO (JobIdx, j)
dequeue lm@LevelMem{..} =
    do jobIdx <- C.readChan (snd lm_queue)
       mJob <-
           atomically $
           do SSet.insert jobIdx lm_inProgress
              SMap.lookup jobIdx lm_jobs
       case mJob of
         Nothing -> dequeue lm
         Just j -> pure (jobIdx, j)

ack :: JobIdx -> LevelMem j -> IO ()
ack jobIdx LevelMem{..} =
    atomically $
    do SSet.delete jobIdx lm_inProgress
       SMap.delete jobIdx lm_jobs

nack :: JobIdx -> LevelMem j -> IO ()
nack jobIdx LevelMem{..} =
    do atomically $ SSet.delete jobIdx lm_inProgress
       C.writeChan (fst lm_queue) jobIdx
