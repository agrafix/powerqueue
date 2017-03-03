{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
module Data.PowerQueue.Worker.Distributed
    ( AuthToken(..), AppVersion(..)
    , WorkMasterConfig(..), ServerErrorEvent(..), launchWorkMaster
    , WorkNodeConfig(..), ClientErrorEvent(..), launchWorkNode, launchReconnectingWorkNode
    )
where

import Control.Exception
import Control.Monad.Trans
import Data.Conduit
import Data.Conduit.Cereal
import Data.Conduit.Network
import Data.IORef
import Data.PowerQueue
import Data.String
import Data.Time.TimeSpan
import Data.Word
import GHC.Generics
import qualified Data.ByteString as BS
import qualified Data.Serialize as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

newtype AuthToken
    = AuthToken { unAuthToken :: T.Text }
    deriving (Show, Eq)

instance S.Serialize AuthToken where
    put (AuthToken t) = S.put $ T.encodeUtf8 t
    get = AuthToken . T.decodeUtf8 <$> S.get

newtype AppVersion
    = AppVersion { unAppVersion :: Word64 }
    deriving (Show, Eq, Ord)

instance S.Serialize AppVersion where
    put (AppVersion av) =
        S.putWord64le av
    get =
        AppVersion <$> S.getWord64le

data Message
    = Message
    { m_version :: !AppVersion
    , m_payload :: !BS.ByteString
    }

instance S.Serialize Message where
    put msg =
        do S.put (m_version msg)
           S.putWord64le (fromIntegral $ BS.length $ m_payload msg)
           S.putByteString (m_payload msg)
    get =
        do vers <- S.get
           bsLen <- S.getWord64le
           bs <- S.getByteString (fromIntegral bsLen)
           pure $ Message vers bs

data ClientPayload
    = CpAuth !AuthToken
    | CpDrain
    | CpCompleted
    | CpRollback
    deriving (Generic)

instance S.Serialize ClientPayload

data ServerPayload j
    = SpJob !j
    | SpHello
    | SpBadAuth
    | SpBadState
    deriving (Generic)

instance S.Serialize j => S.Serialize (ServerPayload j)

-- | Work master configuration
data WorkMasterConfig
    = WorkMasterConfig
    { wmc_host :: !T.Text
    , wmc_port :: !Int
    , wmc_authToken :: !AuthToken
      -- ^ reject all client that do not send this token. See 'wnc_authToken'
    , wmc_appVersion :: !AppVersion
      -- ^ required app version for all clients. See 'wnc_appVersion'
    , wmc_errorHook :: !(ServerErrorEvent -> IO ())
      -- ^ a (non-)critical error occured. Useful for logging
    }

-- | Work master errors
data ServerErrorEvent
    = SeeClientDisconnect
    | SeeClientBadVersion !AppVersion
    | SeeInvalidPayload !String
    deriving (Show, Eq)

data ServerCliState
    = ServerCliState
    { scs_isAuthed :: !Bool
    , scs_rollbackJob :: !(Maybe (IO ()))
    , scs_confirmJob :: !(Maybe (IO ()))
    }

-- | Launch a work master on current thread that will distribute all incoming work on a queue
-- to connecting worker nodes launched via 'launchWorkNode'
launchWorkMaster :: forall j. S.Serialize j => WorkMasterConfig -> QueueBackend j -> IO ()
launchWorkMaster wmc QueueBackend{..} =
    runTCPServer tcpSettings $ \app ->
    appSource app .| conduitGet2 S.get .| handleMessage initCliSt .| conduitPut S.put $$ appSink app
    where
      initCliSt =
          ServerCliState
          { scs_isAuthed = False
          , scs_rollbackJob = Nothing
          , scs_confirmJob = Nothing
          }
      evt cliSt e =
          liftIO $
          do wmc_errorHook wmc e
             case scs_rollbackJob cliSt of
               Just rb -> rb
               Nothing -> pure ()
      srvSend :: Monad m => ServerPayload j -> Conduit a m Message
      srvSend payload =
          yield
          Message
          { m_version = wmc_appVersion wmc
          , m_payload = S.encode payload
          }
      handleMessage cliSt =
          await >>= \mMsg ->
          case mMsg of
            Nothing -> evt cliSt SeeClientDisconnect
            Just message
                | m_version message /= wmc_appVersion wmc ->
                      evt cliSt $ SeeClientBadVersion (m_version message)
                | otherwise ->
                      case S.decode (m_payload message) of
                        Left errMsg -> evt cliSt $ SeeInvalidPayload errMsg
                        Right cliPayload -> handleCliPayload cliSt cliPayload
      handleCliPayload cliSt cliPayload =
          case cliPayload of
            CpAuth tok ->
                do authState <-
                       if tok == wmc_authToken wmc
                       then do srvSend SpHello
                               pure True
                       else do srvSend SpBadAuth
                               pure False
                   handleMessage $ cliSt { scs_isAuthed = authState }
            CpDrain | scs_isAuthed cliSt ->
                do (txId, job) <- liftIO $ qb_lift qb_dequeue
                   srvSend $ SpJob job
                   handleMessage $
                       cliSt
                       { scs_rollbackJob = Just $ qb_lift (qb_rollback txId)
                       , scs_confirmJob = Just $ qb_lift (qb_confirm txId)
                       }
            CpCompleted | scs_isAuthed cliSt ->
                case scs_confirmJob cliSt of
                  Nothing ->
                      do srvSend SpBadState
                         handleMessage cliSt
                  Just ok ->
                      do liftIO ok
                         handleMessage $
                             cliSt { scs_rollbackJob = Nothing, scs_confirmJob = Nothing }
            CpRollback | scs_isAuthed cliSt ->
                case scs_rollbackJob cliSt of
                  Nothing ->
                      do srvSend SpBadState
                         handleMessage cliSt
                  Just rollback ->
                      do liftIO rollback
                         handleMessage $
                             cliSt { scs_rollbackJob = Nothing, scs_confirmJob = Nothing }
            _ ->
                do srvSend SpBadAuth
                   handleMessage cliSt
      tcpSettings =
          serverSettings (wmc_port wmc) (fromString $ T.unpack $ wmc_host wmc)

-- | Work node configuration
data WorkNodeConfig
    = WorkNodeConfig
    { wnc_hostMaster :: !T.Text
      -- ^ host where the work master is running. See 'wmc_host'
    , wnc_portMaster :: !Int
      -- ^ port of work master. See 'wmc_port'
    , wnc_authToken :: !AuthToken
      -- ^ the authentification token. MUST match the masters 'wmc_authToken'!
    , wnc_appVersion :: !AppVersion
      -- ^ the current app version. MUST match the masters 'wmc_appVersion'!
    , wnc_errorHook :: !(ClientErrorEvent -> IO ())
      -- ^ a (non-)critical error occured. Useful for logging
    , wnc_readyHook :: !(IO ())
      -- ^ called once when ready for draining
    }

-- | Work node async errors
data ClientErrorEvent
    = CeeConnClosed
    | CeeBadAuthResponse
    | CeeInvalidAuthResponse
    | CeeInvalidDrainResponse
    | CeeServerBadVersion !AppVersion
    | CeeInvalidPayload !String
    | CeeWorkerException !String
    deriving (Show, Eq)

data ClientState
    = ClientState
    { cs_authed :: !Bool
    }

-- | Launch a worker node on the current thread connecting to a work master launched with
-- 'launchWorkMaster'
launchWorkNode :: forall j. S.Serialize j => WorkNodeConfig -> QueueWorker j -> IO ()
launchWorkNode wnc QueueWorker{..} =
    runTCPClient tcpSettings $ \app ->
    appSource app .| conduitGet2 S.get .| handleMessage initCliSt .| conduitPut S.put $$ appSink app
    where
      tcpSettings =
          clientSettings (wnc_portMaster wnc) (T.encodeUtf8 $ wnc_hostMaster wnc)
      initCliSt =
          ClientState
          { cs_authed = False
          }
      evt _ e =
          liftIO $ wnc_errorHook wnc e
      cliSend :: Monad m => ClientPayload -> Conduit a m Message
      cliSend payload =
          yield
          Message
          { m_version = wnc_appVersion wnc
          , m_payload = S.encode payload
          }
      awaitSrv ::
          MonadIO m
          => ClientState
          -> (ServerPayload j -> Conduit Message m Message)
          -> Conduit Message m Message
      awaitSrv cliSt go =
          do msg <- await
             case msg of
               Nothing -> evt cliSt CeeConnClosed
               Just (Message msgVer msgBsl)
                   | msgVer == wnc_appVersion wnc ->
                         case S.decode msgBsl of
                             Left err -> evt cliSt $ CeeInvalidPayload err
                             Right ok -> go ok
                   | otherwise -> evt cliSt $ CeeServerBadVersion msgVer
      handleMessage cliSt
          | not (cs_authed cliSt) =
                do cliSend (CpAuth $ wnc_authToken wnc)
                   awaitSrv cliSt $ \msg ->
                       case msg of
                         SpHello ->
                             do liftIO $ wnc_readyHook wnc
                                handleMessage $ cliSt { cs_authed = True }
                         _ -> evt cliSt CeeInvalidAuthResponse
          | otherwise = workLoop cliSt
      workLoop cliSt =
          do cliSend CpDrain
             awaitSrv cliSt $ \msg ->
                 case msg of
                   SpJob job ->
                       do execRes <- liftIO $ try $ qw_execute job
                          case execRes of
                            Left (e :: SomeException) ->
                                do evt cliSt $ CeeWorkerException (show e)
                                   cliSend CpRollback
                            Right res ->
                                case res of
                                  JOk -> cliSend CpCompleted
                                  JRetry -> cliSend CpRollback
                          workLoop cliSt
                   SpBadAuth ->
                       evt cliSt CeeBadAuthResponse
                   _ ->
                       evt cliSt CeeInvalidDrainResponse

launchReconnectingWorkNode ::
    forall j. S.Serialize j
    => WorkNodeConfig
    -> (TimeSpan -> IO ())
    -- ^ callback: will retry in 'TimeSpan'. Useful for logging
    -> QueueWorker j
    -> IO ()
launchReconnectingWorkNode wnc retryCallback qw =
    reconnStep (milliseconds 200)
    where
        reconnStep ts =
           do conGood <- newIORef False
              let wnc' =
                      wnc
                      { wnc_readyHook =
                          do writeIORef conGood True
                             wnc_readyHook wnc
                      }
              (e :: Either SomeException ()) <- try $ launchWorkNode wnc' qw
              print e
              retryCallback ts
              sleepTS ts
              wasGood <- readIORef conGood
              reconnStep $
                  if wasGood
                  then milliseconds 200
                  else multiplyTS ts 2
