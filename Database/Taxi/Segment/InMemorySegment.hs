{-|
  Module      : Database.Taxi.Segment.InMemorySegment
  Description : Operations on the in-memory segment
  Stability   : Experimental
  Maintainer  : guha.rabishankar@gmail.com
-}

module Database.Taxi.Segment.InMemorySegment where

import           Data.Binary                 (Binary)
import           Data.Maybe                  (fromMaybe)
import           Data.Set                    (Set)
import qualified Data.Set                    as S
import           Data.Text                   (Text)
import qualified Data.Trie                   as T

import           Database.Taxi.Prelude
import           Database.Taxi.Segment.Flush as F
import           Database.Taxi.Segment.Types

empty :: InMemorySegment p
empty = InMemorySegment T.empty

insert :: Ord p => InMemorySegment p -> Text -> p -> InMemorySegment p
insert (InMemorySegment trie) term p =
   InMemorySegment $ T.insert termBS postings trie
  where
    postings = case T.lookup termBS trie of
      Nothing -> S.singleton p
      Just val -> S.insert p val
    termBS = toByteString term

lookup :: InMemorySegment p -> Text -> Set p
lookup (InMemorySegment trie) term =
  fromMaybe S.empty $ T.lookup (toByteString term) trie

flush :: (Binary p, Binary doc, Ord doc)
         => InMemorySegment p
         -> FilePath
         -> Set doc
         -> (Set p -> Set doc -> (Set p, Set doc))
         -> IO (ExternalSegment p, Set doc)
flush = F.flush
