module Data.PowerQueue.Backend.LevelMemSpec
    ( spec )
where

import Data.PowerQueue
import Data.PowerQueue.Backend.LevelMem

import Control.Concurrent.Async
import Control.Monad
import Data.IORef
import Data.List
import System.IO.Temp
import Test.Hspec
import qualified Data.Serialize as S

dummyRefWorker :: (a -> b -> b) -> IORef b -> QueueWorker a
dummyRefWorker f ioRef =
    newQueueWorker $ \v -> atomicModifyIORef' ioRef $ \i -> (f v i, JOk)

dummyRefQueue :: QueueBackend a ->  (a -> b -> b) -> IORef b -> Queue a
dummyRefQueue be f ioRef =
    newQueue be (dummyRefWorker f ioRef)

dummyCounterQueue :: QueueBackend () -> IORef Int -> Queue ()
dummyCounterQueue be = dummyRefQueue be (\_ i -> i + 1)

dummyReverseAccumQueue :: QueueBackend Int -> IORef [Int] -> Queue Int
dummyReverseAccumQueue be = dummyRefQueue be (:)

binEncoding :: S.Serialize a => JobEncoding a
binEncoding =
    JobEncoding
    { j_encode = S.encode
    , j_decode = S.decode
    }

withTmpDir :: (FilePath -> IO a) -> IO a
withTmpDir = withSystemTempDirectory "lmstestsXXX"

testCfg ::  S.Serialize a => FilePath -> LevelMemCfg a
testCfg fp =
    LevelMemCfg
    { lmc_storageDir = fp
    , lmc_maxQueueSize = 100
    , lmc_jobEncoding = binEncoding
    , lmc_inProgressRecovery = IpRestart
    }

spec :: Spec
spec =
    describe "Level Mem Backend" $
    do it "providing 100 jobs and stepping 100 times works" $
           withTmpDir $ \tmpDir ->
           withLevelMem (testCfg tmpDir) $ \lm ->
           do ref <- newIORef 0
              let queue = dummyCounterQueue (newLevelMemBackend lm) ref
              replicateM_ 100 $ enqueueJob () queue
              replicateM_ 100 $ workStep queue
              result <- readIORef ref
              result `shouldBe` 100
       it "synchronous recovery works" $
           withTmpDir $ \tmpDir ->
           do ref <- newIORef 0
              withLevelMem (testCfg tmpDir) $ \lm ->
                  do let queue = dummyCounterQueue (newLevelMemBackend lm) ref
                     replicateM_ 100 $ enqueueJob () queue
                     replicateM_ 50 $ workStep queue
              withLevelMem (testCfg tmpDir) $ \lm ->
                  do statusMap <- getJobStatusMap lm
                     length statusMap `shouldBe` 50
                     let queue = dummyCounterQueue (newLevelMemBackend lm) ref
                     replicateM_ 50 $ workStep queue
              result <- readIORef ref
              result `shouldBe` 100
       it "asynchronous working and recovery works" $
           withTmpDir $ \tmpDir ->
           do ref <- newIORef []
              withLevelMem (testCfg tmpDir) $ \lm ->
                  do let queue = dummyReverseAccumQueue (newLevelMemBackend lm) ref
                     adder <- async $ forConcurrently_ [1..100] $ \idx -> enqueueJob idx queue
                     worker <- async $ replicateConcurrently_ 10 $ replicateM_ 5 $ workStep queue
                     wait adder
                     wait worker
              withLevelMem (testCfg tmpDir) $ \lm ->
                  do statusMap <- getJobStatusMap lm
                     length statusMap `shouldBe` 50
                     let queue = dummyReverseAccumQueue (newLevelMemBackend lm) ref
                     replicateM_ 50 $ workStep queue
              result <- sort <$> readIORef ref
              result `shouldBe` [1..100]
