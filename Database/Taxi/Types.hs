{-|
  Module      : Database.Taxi.Types
  Description : Type definitions
  Stability   : Experimental
  Maintainer  : guha.rabishankar@gmail.com
-}

{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving    #-}

module Database.Taxi.Types where

import           Control.Applicative     ((<$>), (<*>))
import           Data.Binary
import qualified Data.Text.Encoding      as E
import qualified Data.Text               as T
import           Control.Concurrent.MVar (MVar)

import           Database.Taxi.Segment.Types
import           Database.Taxi.SegmentSet.Types

-- | Wrapper around 'Data.Text'
-- Denotes the key for all operations on this index
newtype Term doc = Term { unKey :: T.Text }
                 deriving (Show, Eq, Ord)

instance Binary (Term doc) where
    put = put . E.encodeUtf8 . unKey
    get = Term <$> E.decodeUtf8 <$> get

-- | Stores the associated information for a 'Term'
-- Consists of 'DocumentId' and other associated data
-- The associated data is represented by a parametrized type /p/
-- /p/ can be term frequency, relevance score, character offset
-- or a sum type representing all of the above
data Posting doc p where
  Posting :: { postingDocId :: doc -- ^ unique document id
             , posting     :: p   -- ^ associated data corresponding to this term
             } -> Posting doc p

deriving instance (Show doc, Show b) => Show (Posting doc b)


-- HACK: Currently Data.Set does not support insertBy, queryBy
-- functions. So we need to have an /Ord/ instance for /a/
-- to have /Set a/
instance Eq doc => Eq (Posting doc b) where
  p1 == p2 = postingDocId p1 == postingDocId p2

instance Ord doc => Ord (Posting doc b) where
  p1 `compare` p2 = postingDocId p1 `compare` postingDocId p2

instance Document doc b => Binary (Posting doc b) where
  put (Posting docId b) = put docId >> put b
  get = Posting <$> get <*> get

-- | Typeclass to capture data types that can be represented as documents
class ( Ord doc, Binary doc, Binary p
      ) => Document doc p where
  -- | Processes a document to retrieve the terms
  -- Returns a list of tuples of /Key/ and /b/ where /b/ can be any
  -- arbitrary information corresponding to the term
  -- Together with /DocumentId/ this is used to construct the list of 'Term'
  getTerms :: doc -> [(Term doc, p)]


-- | Configuration parameters
data IndexConfig  = IndexConfig { baseDirectory :: FilePath  -- ^ Base directory to store files
                                }

-- | Index Handle
data Index doc p = Index
                   { -- | Current configuration of the running index
                     config       :: IndexConfig
                     -- | The in-memory terms of the index
                   , segments     :: MVar (SegmentSet doc (Posting doc p))
                     -- | MVar to hold a in-memory segment while it is being flushed
                   , flushSegment :: MVar (InMemorySegment (Posting doc p))
                   }
