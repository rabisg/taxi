module Database.Taxi.Segment.Merge ( merge
                                   ) where

import Data.Set (Set)

import Database.Taxi.Segment.Types

merge :: ExternalSegment p -> ExternalSegment p
         -> Set doc
         -> (Set p -> Set doc -> (Set p, Set doc))
         -> IO (ExternalSegment p, Set doc)
merge = undefined
