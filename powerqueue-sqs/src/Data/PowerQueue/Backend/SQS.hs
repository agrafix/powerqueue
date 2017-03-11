{-# LANGUAGE RankNTypes #-}
module Data.PowerQueue.Backend.SQS
    ( SqsConfig(..)
    , newSQSBackend
    )
where

import Data.PowerQueue
import Data.Time.TimeSpan
import Network.AWS.Simple
import qualified Control.Monad.Fail as Fail
import qualified Data.Text as T

data SqsConfig j
    = SqsConfig
    { sc_aws :: !AWSHandle
    , sc_queueName :: !T.Text
    , sc_writeJob :: j -> T.Text
    , sc_readJob :: forall m. Fail.MonadFail m => T.Text -> m j
    }

newSQSBackend :: SqsConfig j -> IO (QueueBackend j)
newSQSBackend cfg =
    do qu <- sqsGetQueue (sc_aws cfg) (sc_queueName cfg)
       pure $
           QueueBackend
           { qb_lift = id
           , qb_enqueue = enqueue cfg qu
           , qb_dequeue = dequeue cfg qu
           , qb_confirm = confirm cfg qu
           , qb_rollback = rollback cfg qu
           , qb_reportProgress = repProgress cfg qu
           , qb_progressReportInterval = seconds 40
           }

enqueue :: SqsConfig j -> AWSQueue -> j -> IO Bool
enqueue cfg q job =
    do sqsSendMessage (sc_aws cfg) q $ sc_writeJob cfg job
       pure True

dequeue :: SqsConfig j -> AWSQueue -> IO (MessageHandle, j)
dequeue cfg q =
    do let fetchLoop =
               do msgs <-
                      sqsGetMessage (sc_aws cfg) q $
                      GetMessageCfg
                      { gmc_ackTimeout = minutes 1
                      , gmc_messages = 1
                      , gmc_waitTime = seconds 10
                      }
                  case msgs of
                    [] -> fetchLoop
                    (x : _) -> pure x
       msg <- fetchLoop
       payload <- sc_readJob cfg (sm_payload msg)
       pure (sm_handle msg, payload)

confirm :: SqsConfig j -> AWSQueue -> MessageHandle -> IO ()
confirm cfg q handle =
    sqsAckMessage (sc_aws cfg) q handle

repProgress :: SqsConfig j -> AWSQueue -> MessageHandle -> IO ()
repProgress cfg q handle =
    sqsChangeMessageTimeout (sc_aws cfg) q handle (minutes 1)

rollback :: SqsConfig j -> AWSQueue -> MessageHandle -> IO ()
rollback _ _ _ = pure ()
