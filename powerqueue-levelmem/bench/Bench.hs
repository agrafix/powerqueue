module Main where

import Data.PowerQueue
import Data.PowerQueue.Backend.LevelMem

import Control.Monad
import Criterion.Main
import System.IO.Temp
import qualified Data.Serialize as S

binEncoding :: S.Serialize a => JobEncoding a
binEncoding =
    JobEncoding
    { j_encode = S.encode
    , j_decode = S.decode
    }

nopWorker :: QueueBackend () -> Queue ()
nopWorker be =
    newQueue be $ newQueueWorker $ \() -> pure JOk

main :: IO ()
main =
    withSystemTempDirectory "lmsbenchXXX" $ \tempDir ->
    withLevelMem tempDir 200000 binEncoding $ \lm ->
    do let q = nopWorker (newLevelMemBackend lm)
       putStrLn "Filling queue with 100k entries"
       replicateM_ 100000 $ enqueueJob () q
       putStrLn "Ready."
       defaultMain
           [ bgroup "enqueue dequeue performance"
             [ bench "enqueue + dequeue" $ nfIO $ enqueueJob () q >> workStep q
             ]
           ]
