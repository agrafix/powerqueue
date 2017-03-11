{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE BangPatterns #-}
module Data.PowerQueue.Backend.LevelMem
    ( -- * Data container
      withLevelMem, LevelMemCfg(..), InProgressCfg(..), JobEncoding(..), LevelMem
    , JobStatus(..), getJobStatusMap, getJobStatus
      -- * Actual backend for powerqueue
    , newLevelMemBackend
    )
where

import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Maybe
import Data.Monoid
import Data.PowerQueue
import Data.Time.TimeSpan
import Data.Word
import System.FilePath
import qualified Control.Concurrent.Chan.Unagi.Bounded as C
import qualified Data.ByteString as BS
import qualified Data.DList as DL
import qualified Data.Serialize as S
import qualified Database.LevelDB.Base as L
import qualified ListT as L
import qualified STMContainers.Map as SMap
import qualified STMContainers.Set as SSet

type Chan a = (C.InChan a, C.OutChan a)
type JobIdx = Word64

data QueueAction j
    = QaEnqueue !JobIdx !j
    | QaDequeue !JobIdx
    | QaConfirm !JobIdx
    | QaRollback !JobIdx

databaseOps :: (j -> BS.ByteString) -> QueueAction j -> [L.BatchOp]
databaseOps serJob qa =
    case qa of
      QaEnqueue idx job ->
          [ L.Put (mKey idx) (serJob job)
          , L.Put (qKey idx) ""
          ]
      QaDequeue idx ->
          [ L.Del (qKey idx)
          , L.Put (pKey idx) ""
          ]
      QaConfirm idx ->
          [ L.Del (pKey idx)
          , L.Del (mKey idx)
          ]
      QaRollback idx ->
          [ L.Del (pKey idx)
          , L.Put (qKey idx) ""
          ]


type DbKeyFun = Word64 -> BS.ByteString

qKey :: DbKeyFun
qKey idx = "q_" <> S.encode idx

mKey :: DbKeyFun
mKey idx = "map_" <> S.encode idx

pKey :: DbKeyFun
pKey idx = "p_" <> S.encode idx

data Terminate
    = Continue
    | Terminate

databaseWorkerStep ::
    L.DB -> (j -> BS.ByteString) -> TVar Terminate -> TQueue (QueueAction j) -> IO Bool
databaseWorkerStep db serJob termV q =
    do (dbOps, goAgain) <-
           atomically $
           do op <- tryReadTQueue q
              term <- readTVar termV
              case (op, term) of
                (Just o, _) ->
                    (,)
                    <$> (concatMap (databaseOps serJob) . DL.toList <$> drainLoop (DL.singleton o))
                    <*> pure True
                (Nothing, Continue) -> retry
                (Nothing, Terminate) -> pure ([], False)
       L.write db (L.WriteOptions True) dbOps
       pure goAgain
    where
        drainLoop !accum =
            do mOp <- tryReadTQueue q
               case mOp of
                 Nothing -> pure accum
                 Just nextOp -> drainLoop (DL.snoc accum nextOp)

databaseWorker :: L.DB -> (j -> BS.ByteString) -> TVar Terminate -> TQueue (QueueAction j) -> IO ()
databaseWorker db serJob termV q =
    do goAgain <- databaseWorkerStep db serJob termV q
       if goAgain
          then databaseWorker db serJob termV q
          else pure ()

databaseRecovery ::
    InProgressCfg
    -> TVar JobIdx
    -> SMap.Map JobIdx j
    -> Chan JobIdx
    -> SSet.Set JobIdx
    -> (BS.ByteString -> Either String j)
    -> L.DB
    -> IO ()
databaseRecovery inPCfg jobIdxV jobMap jobQueue progressSet parseJob db =
    L.withIter db L.defaultReadOptions $ \it ->
    do populateMap it
       populateSet it 2 "p_" $ \jobIdx ->
           case inPCfg of
             IpRecover -> atomically $ SSet.insert jobIdx progressSet
             IpRestart -> C.writeChan (fst jobQueue) jobIdx
             IpForget -> atomically $ SMap.delete jobIdx jobMap
       populateSet it 2 "q_" $ C.writeChan (fst jobQueue)
    where
      populateMap it =
          do L.iterSeek it "map_"
             let workLoop !maxIdx =
                     do k <- L.iterKey it
                        v <- L.iterValue it
                        case (,) <$> k <*> v of
                          Just (kBs, vBs)
                            | BS.take 4 kBs == "map_" ->
                                case (,) <$> S.decode (BS.drop 4 kBs) <*> parseJob vBs of
                                  Left err -> fail err
                                  Right (jobIdx, job) ->
                                      do atomically $ SMap.insert job jobIdx jobMap
                                         L.iterNext it
                                         workLoop (if jobIdx > maxIdx then jobIdx else maxIdx)
                          _ -> pure maxIdx
             jobIdx <- workLoop 0
             atomically $ writeTVar jobIdxV (jobIdx + 1)
      populateSet it dropPrefix prefix onVal =
          do L.iterSeek it prefix
             let fetchLoop =
                     do k <- L.iterKey it
                        case k of
                          Just bs | BS.take dropPrefix bs == prefix ->
                              case S.decode (BS.drop dropPrefix bs) of
                                Left _ -> pure ()
                                Right ok ->
                                    do void $ onVal ok
                                       L.iterNext it
                                       fetchLoop
                          _ -> pure ()
             fetchLoop

-- | Binary encoding of a single job. Note that it is highly recommended to use a
-- backwards compatible decoder, otherwise the persistent state can not be read.
-- You could use safecopy or an appropriate cereal, binary, aeson or other decoding.
data JobEncoding j
    = JobEncoding
    { j_encode :: j -> BS.ByteString
    , j_decode :: BS.ByteString -> Either String j
    }

-- | Behavoir for in progress jobs after loading the state from disk on launch
data InProgressCfg
    = IpRecover
      -- ^ mark the job as in progress
    | IpRestart
      -- ^ add the job to the queue
    | IpForget
      -- ^ discard the job

data LevelMemCfg j
    = LevelMemCfg
    { lmc_storageDir :: !FilePath
      -- ^ directory for persistence
    , lmc_maxQueueSize :: !Int
      -- ^ maximum size of queue, succeeding enqueues will block
    , lmc_jobEncoding :: !(JobEncoding j)
      -- ^ binary encoding of jobs
    , lmc_inProgressRecovery :: !InProgressCfg
      -- ^ how should in progress jobs be handled after a restart while loading from disk
    }

-- | Create a new data container for in memory job tracking and leveldb disk persistence.
-- Provide a 'FilePath' for storing the data, an 'Int' as maximum queue size and
-- a 'JobEncoding' for individual job encoding.
withLevelMem :: LevelMemCfg j -> (LevelMem j -> IO a) -> IO a
withLevelMem LevelMemCfg{..} action =
    L.withDB (lmc_storageDir </> "queue.db") opts $ \dbHandle ->
    do queueKey <- newTVarIO 0
       jobMap <- SMap.newIO
       jobQueue <- C.newChan lmc_maxQueueSize
       progressSet <- SSet.newIO
       databaseRecovery
           lmc_inProgressRecovery queueKey jobMap jobQueue progressSet
           (j_decode lmc_jobEncoding) dbHandle
       persistQueue <- newTQueueIO
       termVar <- newTVarIO Continue
       let alloc = async $ databaseWorker dbHandle (j_encode lmc_jobEncoding) termVar persistQueue
           dealloc aHandle =
               do atomically $ writeTVar termVar Terminate
                  wait aHandle
       bracket alloc dealloc $ \_ ->
           action
           LevelMem
           { lm_queueKey = queueKey
           , lm_jobs = jobMap
           , lm_queue = jobQueue
           , lm_inProgress = progressSet
           , lm_persistQueue = persistQueue
           }
    where
      opts = L.defaultOptions { L.createIfMissing = True }

-- | The data container for tracking jobs and persisting them to disk
data LevelMem j
    = LevelMem
    { lm_queueKey :: !(TVar JobIdx)
    , lm_jobs :: !(SMap.Map JobIdx j)
    , lm_queue :: !(Chan JobIdx)
    , lm_inProgress :: !(SSet.Set JobIdx)
    , lm_persistQueue :: !(TQueue (QueueAction j))
    }

data JobStatus
    = JQueued
      -- ^ job is enqueued, not being worked on
    | JInProgress
      -- ^ job currently being worked on
    deriving (Show, Eq)

-- | Get a snapshot of the current state of all jobs.
getJobStatusMap :: LevelMem j -> IO [(j, JobStatus)]
getJobStatusMap LevelMem{..} =
    atomically $ L.fold folder [] $ SMap.stream lm_jobs
    where
      folder jobs (k, v) =
          do progress <- SSet.lookup k lm_inProgress
             pure ((v, if progress then JInProgress else JQueued) : jobs)

-- | Get the job status for a job. Note that this is a potentially
-- expensive operation as all known jobs must be traversed. A more efficient
-- handling would be to call 'getJobStatusMap' once beforce launching the
-- worker and then tracking status manually within the worker.
getJobStatus :: Eq j => j -> LevelMem j -> IO (Maybe JobStatus)
getJobStatus job LevelMem{..} =
    -- not implemented using 'getJobStatusMap' to allow early termination
    atomically $ L.foldMaybe folder Nothing $ SMap.stream lm_jobs
    where
      folder st (k, v)
          | isJust st = pure Nothing
          | v /= job = pure (Just st)
          | otherwise =
                do progress <- SSet.lookup k lm_inProgress
                   pure $ Just $ Just $ if progress then JInProgress else JQueued

-- | Create a queue backend from 'LevelMem'
newLevelMemBackend :: LevelMem j -> QueueBackend j
newLevelMemBackend levelMem =
    QueueBackend
    { qb_lift = id
    , qb_enqueue = flip enqueue levelMem
    , qb_dequeue = dequeue levelMem
    , qb_confirm = flip ack levelMem
    , qb_rollback = flip nack levelMem
    , qb_progressReportInterval = hours 1
    , qb_reportProgress = const $ pure ()
    }

enqueue :: j -> LevelMem j -> IO Bool
enqueue job lm@LevelMem{..} =
    do ix <-
           atomically $
           do key <- readTVar lm_queueKey
              SMap.insert job key lm_jobs
              writeTVar lm_queueKey (key+1)
              writeTQueue lm_persistQueue (QaEnqueue key job)
              return key
       ok <- C.tryWriteChan (fst lm_queue) ix
       unless ok $ ack ix lm
       pure ok

dequeue :: LevelMem j -> IO (JobIdx, j)
dequeue lm@LevelMem{..} =
    do jobIdx <- C.readChan (snd lm_queue)
       mJob <-
           atomically $
           do SSet.insert jobIdx lm_inProgress
              writeTQueue lm_persistQueue (QaDequeue jobIdx)
              SMap.lookup jobIdx lm_jobs
       case mJob of
         Nothing -> dequeue lm
         Just j -> pure (jobIdx, j)

ack :: JobIdx -> LevelMem j -> IO ()
ack jobIdx LevelMem{..} =
    atomically $
    do SSet.delete jobIdx lm_inProgress
       SMap.delete jobIdx lm_jobs
       writeTQueue lm_persistQueue (QaConfirm jobIdx)

nack :: JobIdx -> LevelMem j -> IO ()
nack jobIdx LevelMem{..} =
    do atomically $
           do SSet.delete jobIdx lm_inProgress
              writeTQueue lm_persistQueue (QaRollback jobIdx)
       C.writeChan (fst lm_queue) jobIdx
