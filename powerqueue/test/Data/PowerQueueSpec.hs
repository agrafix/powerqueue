module Data.PowerQueueSpec
    ( spec )
where

import Data.PowerQueue

import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Monad
import Data.IORef
import Test.Hspec

dummyRefWorker :: (a -> b -> b) -> IORef b -> QueueWorker a
dummyRefWorker f ioRef =
    newQueueWorker $ \v -> atomicModifyIORef' ioRef $ \i -> (f v i, JOk)

dummyRefQueue :: (a -> b -> b) -> IORef b -> IO (Queue a)
dummyRefQueue f ioRef =
    do be <- basicChanBackend
       pure $ newQueue be (dummyRefWorker f ioRef)

dummyCounterQueue :: IORef Int -> IO (Queue ())
dummyCounterQueue = dummyRefQueue (\_ i -> i + 1)

dummyStmCounterQueue :: TVar Int -> IO (Queue ())
dummyStmCounterQueue tv =
    do be <- basicChanBackend
       pure $ newQueue be $ newQueueWorker $ \() ->
           do atomically $ modifyTVar' tv (+1)
              pure JOk

dummyReverseAccumQueue :: IORef [Int] -> IO (Queue Int)
dummyReverseAccumQueue = dummyRefQueue (:)

spec :: Spec
spec =
    do specCore
       specLocalWorker

specLocalWorker :: Spec
specLocalWorker =
    describe "localQueueWorker" $
    it "should work with 100 jobs" $
    do ref <- atomically $ newTVar 0
       queue <- dummyStmCounterQueue ref
       handler <- async $ localQueueWorker (LocalWorkerConfig 10) queue
       replicateM_ 100 $ enqueueJob () queue
       result <-
           atomically $
           do val <- readTVar ref
              when (val /= 100) retry
              pure val
       cancel handler
       result `shouldBe` 100

specCore :: Spec
specCore =
    describe "PowerQueue Core" $
    do it "providing 100 jobs and stepping 100 times works" $
           do ref <- newIORef 0
              queue <- dummyCounterQueue ref
              replicateM_ 100 $ enqueueJob () queue
              replicateM_ 100 $ workStep queue
              result <- readIORef ref
              result `shouldBe` 100
       it "all jobs are executed" $
           do ref <- newIORef []
              queue <- dummyReverseAccumQueue ref
              let out = [1..100]
              forM_ out $ flip enqueueJob queue
              replicateM_ 100 $ workStep queue
              result <- reverse <$> readIORef ref
              result `shouldBe` out
       it "failed jobs result in reenqueue" $
           do be <- basicChanBackend
              crashed <- newIORef False
              out <- newIORef False
              let worker :: QueueWorker ()
                  worker =
                      newQueueWorker $ \() ->
                      do x <- readIORef crashed
                         writeIORef crashed True
                         when x $ writeIORef out True
                         pure (if x then JOk else JRetry)
              let queue = newQueue be worker
              enqueueJob () queue
              workStep queue
              workStep queue
              result <- readIORef out
              result `shouldBe` True
       it "unhandled crashes result in reenqueue" $
           do be <- basicChanBackend
              crashed <- newIORef False
              out <- newIORef False
              let worker :: QueueWorker ()
                  worker =
                      newQueueWorker $ \() ->
                      do x <- readIORef crashed
                         writeIORef crashed True
                         unless x $ fail "OOOOPS"
                         writeIORef out True
                         pure JOk
              let queue = newQueue be worker
              enqueueJob () queue
              workStep queue
              workStep queue
              result <- readIORef out
              result `shouldBe` True
