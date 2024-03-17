{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ViewPatterns #-}

module Pinch.Transport
  ( Transport(..)
  , framedTransport
  , unframedTransport
  , Connection(..)
  , ReadResult(..)
  ) where

import Data.IORef (newIORef, readIORef, writeIORef)
import Network.Socket (Socket)
import Network.Socket.ByteString (sendAll, recv)
import System.IO (Handle)

import qualified Data.ByteString as BS
import qualified Data.Serialize.Get as G

import qualified Pinch.Internal.Builder as B
import Debug.Trace

class Connection c where
  -- | Returns available bytes, or an empty bytestring if EOF was reached.
  cGetSome :: c -> IO BS.ByteString
  -- | Writes the given builder.
  cPut :: c -> B.Builder -> IO ()

instance Connection Handle where
  cPut c b = BS.hPut c (B.runBuilder b)
  cGetSome h = BS.hGetSome h 1024

instance Connection Socket where
  cPut c b = sendAll c (B.runBuilder b)
  cGetSome s = traceCtx "cGetSome" <$> recv s 4096

data ReadResult a
  = RRSuccess a
  | RRFailure String
  | RREOF
  deriving (Eq, Show)

-- | A bidirectional transport to read/write messages from/to.
data Transport
  = Transport
  { writeMessage :: B.Builder -> IO ()
  , readMessage  :: forall a . G.Get a -> IO (ReadResult a)
  }

instance Show B.Builder where
  show = show . B.runBuilder

-- | Creates a thrift framed transport. See also <https://github.com/apache/thrift/blob/master/doc/specs/thrift-rpc.md#framed-vs-unframed-transport>.
framedTransport :: Connection c => c -> IO Transport
framedTransport c = do
  readBuffer <- newIORef mempty
  pure $ Transport writeMsg (readMsg readBuffer) where
  writeMsg msg = do
    cPut c $ traceCtx "Writing" $ B.int32BE (fromIntegral $ B.getSize msg) <> msg
  readMsg readBuffer parser = do
    let 
      frameParser = do 
        size <- G.getInt32be
        G.isolateLazy (fromIntegral size) parser
    
    initial <- traceCtx "framed read initial" <$> readIORef readBuffer
    (traceCtx "framedTransport leftovers" -> leftovers, r) <- runGetWith (cGetSome c) frameParser initial
    writeIORef readBuffer $! leftovers
    pure r

-- | Creates a thrift unframed transport. See also <https://github.com/apache/thrift/blob/master/doc/specs/thrift-rpc.md#framed-vs-unframed-transport>.
unframedTransport :: Connection c => c -> IO Transport
unframedTransport c = do
  -- As we do not know how long messages are,
  -- we may read more data then the current message needs.
  -- We keep the leftovers in a buffer so that we may use them
  -- when reading the next message.
  readBuffer <- newIORef mempty
  pure $ Transport writeMsg (readMsg readBuffer)
  where
    writeMsg = cPut c

    readMsg buf p = do
      initial <- readIORef buf
      (leftovers, r) <- runGetWith (cGetSome c) p initial
      writeIORef buf $! leftovers
      pure r

traceCtx :: Show a => String -> a -> a
traceCtx ctx a = trace (ctx ++ ": " ++ show a) a

-- | Runs a Get parser incrementally, reading more input as necessary until a successful parse
-- has been achieved.
runGetWith :: IO BS.ByteString -> G.Get a -> BS.ByteString -> IO (BS.ByteString, ReadResult a)
runGetWith getBs p initial = go 0 (G.runGetPartial p initial)
  where
    go n r = case r of
      G.Fail err bs -> do
        trace ("runGetWith " ++ show n ++ " fail: " ++ show err) <$> pure (bs, RRFailure err)
      G.Done a bs -> do
        trace ("runGetWith " ++ show n ++ " done: leftovers: " ++ show bs) <$> pure (bs, RRSuccess a)
      G.Partial cont -> do
        bs <- traceCtx ("runGetWith " ++ show n) <$> getBs
        if BS.null bs
          then
            -- EOF
            pure (bs, RREOF)
          else
            go (succ n) $ cont bs
