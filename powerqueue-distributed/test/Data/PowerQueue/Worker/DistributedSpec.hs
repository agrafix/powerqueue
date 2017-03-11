{-# LANGUAGE OverloadedStrings #-}
module Data.PowerQueue.Worker.DistributedSpec
    ( spec )
where

import Data.PowerQueue
import Data.PowerQueue.Worker.Distributed

import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Monad
import Data.Monoid
import Test.Hspec

dummyStmCounterQueue :: TVar Int -> IO (Queue ())
dummyStmCounterQueue tv =
    do be <- basicChanBackend
       pure $ newQueue be $ newQueueWorker $ \() ->
           do atomically $ modifyTVar' tv (+1)
              pure JOk

spec :: Spec
spec =
    do specWorker

specWorker :: Spec
specWorker =
    describe "specWorker" $
    do it "should work in a simple smoke test" $
           do ref <- atomically $ newTVar 0
              queue <- dummyStmCounterQueue ref
              master <- async $ launchWorkMaster masterCfg (getQueueBackend queue)
              slaves <-
                  forM [1..4] $ \idx -> async $ launchWorkNode (nodeCfg idx) (getQueueWorker queue)
              replicateM_ 100 $ enqueueJob () queue `shouldReturn` True
              result <-
                  atomically $
                  do val <- readTVar ref
                     when (val /= 100) retry
                     pure val
              cancel master
              mapM_ cancel slaves
              result `shouldBe` 100

nodeCfg :: Int -> WorkNodeConfig
nodeCfg idx =
    WorkNodeConfig
    { wnc_hostMaster = "127.0.0.1"
    , wnc_portMaster = 9876
    , wnc_authToken = AuthToken "ABC"
    , wnc_appVersion = AppVersion 1
    , wnc_errorHook = \x -> putStrLn $ "[cli" <> show idx <> "] " <> show x
    , wnc_readyHook = putStrLn $ "[cli" <> show idx <> "] READY!"
    }

masterCfg :: WorkMasterConfig
masterCfg =
    WorkMasterConfig
    { wmc_host = "127.0.0.1"
    , wmc_port = 9876
    , wmc_authToken = AuthToken "ABC"
    , wmc_appVersion = AppVersion 1
    , wmc_errorHook = putStrLn . show
    }
