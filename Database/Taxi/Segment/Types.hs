{-|
  Module      : Database.Taxi.Segment.Types
  Description : Type definitions
  Stability   : Experimental
  Maintainer  : guha.rabishankar@gmail.com
-}

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RecordWildCards #-}

module Database.Taxi.Segment.Types where

import           Control.Applicative ((<$>), (<*>))
import           Data.Binary         (Binary, put, get)
import           Data.ByteString     (ByteString)
import           Data.Set            (Set)
import           Data.Trie           (Trie)
import           Data.UUID           (UUID)
import           GHC.Int             (Int64)

import Database.Taxi.Segment.BinaryHelper

-- | Offset (bytes) into the file
type Offset = Int64

newtype Dictionary p = Dictionary { unDict :: Trie Offset }
                       deriving Binary

type SegmentId = UUID

data InMemorySegment p = InMemorySegment (Trie (Set p))

data ExternalSegment p = ExternalSegment { segmentId  :: SegmentId
                                         , dictionary :: Dictionary p
                                         }

-- | __IIEntry__ stands for Inverted Index Entry and is the basic unit of an inverted index.
-- It is a mapping from term to postings list where additionally
-- posting(s) are stored in sorted order for easier manipulation
data IIEntry p = IIEntry { iiTerm    :: ByteString
                           -- ^ key (string) for this term
                         , iiPostingsList :: [p]
                           -- ^ List of posting corresponding to this key
                         } deriving Show

instance Binary p => Binary (IIEntry p) where
  put IIEntry{..} = put iiTerm >> putListOf put iiPostingsList
  get = IIEntry <$> get <*> getListOf get
