{-|
  Module      : Database.Taxi.Prelude
  Description : Commonly used functions
  Stability   : Experimental
  Maintainer  : guha.rabishankar@gmail.com
-}

{-# LANGUAGE CPP #-}

module Database.Taxi.Prelude ( modifyMVarPure
                             , toByteString
                             , fromByteString
                             , tryReadMVar
                             ) where

import           Control.Concurrent.MVar (MVar, modifyMVar_, tryTakeMVar, tryPutMVar)
import           Data.ByteString         (ByteString)
import           Data.Text               (Text)
import           Data.Text.Encoding      (encodeUtf8, decodeUtf8)

modifyMVarPure :: MVar a -> (a -> a) -> IO ()
modifyMVarPure mVar f = modifyMVar_ mVar $ \a -> return . f $ a

toByteString :: Text -> ByteString
toByteString = encodeUtf8

fromByteString :: ByteString -> Text
fromByteString = decodeUtf8

#if MIN_VERSION_base(4,7,0)
import Control.Concurrent.MVar (tryReadMVar)
#else
tryReadMVar :: MVar a -> IO (Maybe a)
tryReadMVar mv = go =<< tryTakeMVar mv
  where
    go Nothing = return Nothing
    go (Just v) = do
      put <- tryPutMVar mv v
      if put then
        return (Just v)
        else error "Internal Error: tryReadMVar"
#endif
