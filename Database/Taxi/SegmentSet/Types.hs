{-|
  Module      : Database.Taxi.SegmentSet.Types
  Description : Type definitions for Segment Set
  Stability   : Experimental
  Maintainer  : guha.rabishankar@gmail.com
-}

module Database.Taxi.SegmentSet.Types where

import           Data.Map.Strict (Map)
import           Data.Set        (Set)

import Database.Taxi.Segment.Types

data SegmentSet doc p = SegmentSet
                        { segSetBaseDir :: FilePath
                        , inMemorySegment :: InMemorySegment p
                        , externalSegments :: Map SegmentId (ExternalSegment p)
                        , deletedDocs :: Set doc
                        }
